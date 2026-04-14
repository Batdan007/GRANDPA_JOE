"""
Main handicapping model for GRANDPA_JOE.
XGBoost-based prediction with fallback to morning line odds.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from grandpa_joe.path_manager import PathManager

logger = logging.getLogger(__name__)

try:
    import numpy as np
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False


@dataclass
class HorseRanking:
    """A ranked horse in a race."""
    rank: int
    entry_id: int
    horse_name: str
    post_position: int
    win_probability: float
    place_probability: float
    show_probability: float
    confidence: float
    morning_line_odds: float
    features: Dict


class GrandpaJoeHandicapper:
    """
    The core handicapping engine.
    Uses XGBoost when trained model available, falls back to morning line.
    """

    MODEL_VERSION = "0.1.0"

    def __init__(self, brain, model_config=None):
        self.brain = brain
        self.config = model_config
        self.model = None
        self._load_model()

    def _load_model(self):
        """Load trained model if available."""
        if not XGBOOST_AVAILABLE:
            logger.info("XGBoost not installed — using morning line fallback")
            return

        # Check local data dir first, then fall back to repo-bundled model
        model_path = PathManager.MODELS_DIR / "handicapper.json"
        if not model_path.exists():
            # Fall back to repo-bundled model (from git pull)
            repo_model = Path(__file__).resolve().parent.parent.parent / "trained_models" / "handicapper.json"
            if repo_model.exists():
                model_path = repo_model

        if model_path.exists():
            try:
                self.model = xgb.XGBRegressor()
                self.model.load_model(str(model_path))
                logger.info(f"Loaded trained model from {model_path}")
            except Exception as e:
                logger.warning(f"Failed to load model: {e}")
                self.model = None
        else:
            logger.info("No trained model found — using morning line fallback")

    def predict(self, race_id: int) -> List[Dict]:
        """
        Predict rankings for a race.

        Returns list of dicts sorted by predicted rank:
        [{rank, entry_id, horse_name, win_probability, confidence, ...}]
        """
        if self.model and PANDAS_AVAILABLE:
            return self._predict_with_model(race_id)
        else:
            return self._predict_with_morning_line(race_id)

    def _predict_with_model(self, race_id: int) -> List[Dict]:
        """Use trained XGBoost model for predictions."""
        from grandpa_joe.models.features import build_features_for_race, FEATURE_NAMES

        df, entries = build_features_for_race(self.brain, race_id)
        if df is None or df.empty:
            return self._predict_with_morning_line(race_id)

        # Extract feature matrix
        feature_cols = [c for c in FEATURE_NAMES if c in df.columns]
        if not feature_cols:
            return self._predict_with_morning_line(race_id)

        X = df[feature_cols].fillna(0)

        # Predict (lower score = better rank)
        predictions = self.model.predict(X)

        # Build rankings
        rankings = []
        for i, (_, row) in enumerate(df.iterrows()):
            pred_score = float(predictions[i])
            rankings.append({
                "entry_id": int(row.get("entry_id", 0)),
                "horse_name": row.get("horse_name", "Unknown"),
                "post_position": int(entries[i].get("post_position", 0)),
                "predicted_score": pred_score,
                "morning_line_odds": float(row.get("morning_line_odds", 10)),
                "features": {c: float(row[c]) for c in feature_cols},
            })

        # Sort by predicted finish position (lower = better)
        rankings.sort(key=lambda x: x["predicted_score"])

        # Convert to probabilities and assign ranks
        return self._scores_to_rankings(rankings)

    def _predict_with_morning_line(self, race_id: int) -> List[Dict]:
        """Fallback: rank by morning line odds (lower odds = more likely)."""
        race = self.brain.get_race(race_id)
        if not race:
            return []

        entries = race.get("entries", [])
        if not entries:
            return []

        # Sort by morning line odds (favorites first)
        sorted_entries = sorted(
            entries,
            key=lambda e: e.get("morning_line_odds") or 99.0
        )

        rankings = []
        for entry in sorted_entries:
            ml = entry.get("morning_line_odds") or 10.0
            rankings.append({
                "entry_id": entry["id"],
                "horse_name": entry["horse_name"],
                "post_position": entry.get("post_position", 0),
                "predicted_score": ml,
                "morning_line_odds": ml,
                "features": {},
            })

        return self._scores_to_rankings(rankings)

    def _scores_to_rankings(self, sorted_rankings: List[Dict]) -> List[Dict]:
        """Convert sorted scores to rankings with probabilities."""
        n = len(sorted_rankings)
        if n == 0:
            return []

        # Convert scores to rough probabilities using softmax-like approach
        scores = [r["predicted_score"] for r in sorted_rankings]
        min_score = min(scores)
        max_score = max(scores)
        spread = max_score - min_score if max_score != min_score else 1

        # Invert scores (lower predicted finish = higher win prob)
        inv_scores = [(max_score - s + 0.1) for s in scores]
        total = sum(inv_scores)

        result = []
        for i, r in enumerate(sorted_rankings):
            win_prob = inv_scores[i] / total if total > 0 else 1.0 / n
            # Place/show probabilities (approximate)
            place_prob = min(win_prob * 1.8, 0.95)
            show_prob = min(win_prob * 2.5, 0.98)

            # Confidence based on data availability
            has_features = bool(r.get("features"))
            confidence = 0.6 if has_features else 0.3

            result.append({
                "rank": i + 1,
                "entry_id": r["entry_id"],
                "horse_name": r["horse_name"],
                "post_position": r["post_position"],
                "win_probability": round(win_prob, 4),
                "place_probability": round(place_prob, 4),
                "show_probability": round(show_prob, 4),
                "confidence": confidence,
                "morning_line_odds": r["morning_line_odds"],
                "features": r.get("features", {}),
            })

        return result

    def save_predictions(self, race_id: int, rankings: List[Dict]):
        """Persist predictions to brain."""
        for r in rankings:
            self.brain.store_prediction(
                race_id=race_id,
                entry_id=r["entry_id"],
                predicted_rank=r["rank"],
                win_probability=r["win_probability"],
                place_probability=r["place_probability"],
                show_probability=r["show_probability"],
                confidence=r["confidence"],
                model_version=self.MODEL_VERSION,
                features_snapshot=r.get("features", {}),
            )
