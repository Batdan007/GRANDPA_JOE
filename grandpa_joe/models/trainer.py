"""
Model training workflow for GRANDPA_JOE.
Trains XGBoost on historical race data from the brain.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Optional

from grandpa_joe.path_manager import PathManager

logger = logging.getLogger(__name__)

try:
    import numpy as np
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False

try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False


def train_model(brain, model_config=None, before_date: Optional[str] = None) -> Dict:
    """
    Train the handicapping model on historical data.

    Args:
        brain: RacingBrain instance
        model_config: model settings
        before_date: optional YYYY-MM-DD cutoff — only train on races strictly
            before this date. Required for honest backtest/holdout validation.

    Returns dict of metrics.
    """
    if not ML_AVAILABLE:
        raise ImportError("pandas/scikit-learn not installed. Run: pip install grandpa-joe[ml]")
    if not XGBOOST_AVAILABLE:
        raise ImportError("xgboost not installed. Run: pip install grandpa-joe[ml]")

    from grandpa_joe.models.features import FEATURE_NAMES, build_features_for_entry

    logger.info("Starting model training...")

    # Collect training data: sample of races with results
    # With 130K+ races, processing all would take days.
    # 5000 races (~50K entries) gives excellent model quality in minutes.
    MAX_RACES = 1000

    conn = brain._connect()
    try:
        date_filter = "AND ra.race_date < ?" if before_date else ""
        params = (before_date,) if before_date else ()

        total_races = conn.execute(f"""
            SELECT COUNT(DISTINCT ra.id)
            FROM races ra
            JOIN entries e ON e.race_id = ra.id
            JOIN results r ON r.entry_id = e.id
            WHERE 1=1 {date_filter}
        """, params).fetchone()[0]

        races = conn.execute(f"""
            SELECT DISTINCT ra.id, ra.surface, ra.distance_furlongs,
                   ra.track_condition, ra.class_level,
                   t.code as track_code
            FROM races ra
            JOIN tracks t ON ra.track_id = t.id
            JOIN entries e ON e.race_id = ra.id
            JOIN results r ON r.entry_id = e.id
            WHERE 1=1 {date_filter}
            ORDER BY RANDOM()
            LIMIT {MAX_RACES}
        """, params).fetchall()

        if len(races) < 10:
            return {"error": "Not enough data", "races_found": len(races),
                    "minimum_required": 10}

        logger.info(f"Sampled {len(races)} of {total_races} races for training")

        # Build feature/target pairs
        all_features = []
        all_targets = []
        processed_races = set()

        import time as _time
        _t0 = _time.time()
        for ri, race_row in enumerate(races):
            race_id = race_row["id"]
            if race_id in processed_races:
                continue
            processed_races.add(race_id)

            if ri % 50 == 0:
                elapsed = _time.time() - _t0
                print(f"  Processing race {ri+1}/{len(races)} "
                      f"({len(all_features)} samples, {elapsed:.0f}s)", flush=True)

            race_dict = dict(race_row)

            entries = conn.execute("""
                SELECT e.*, h.name as horse_name,
                       r.finish_position
                FROM entries e
                JOIN horses h ON e.horse_id = h.id
                JOIN results r ON r.entry_id = e.id
                WHERE e.race_id = ? AND e.scratched = 0
            """, (race_id,)).fetchall()

            for entry in entries:
                entry_dict = dict(entry)
                entry_dict["race_id"] = race_id

                try:
                    features = build_features_for_entry(conn, entry_dict, race_dict)
                    feature_vec = [features.get(f, 0) for f in FEATURE_NAMES]
                    target = entry_dict["finish_position"]

                    all_features.append(feature_vec)
                    all_targets.append(target)
                except Exception as e:
                    logger.debug(f"Skipping entry {entry_dict.get('id')}: {e}")

        if len(all_features) < 50:
            return {"error": "Not enough training samples",
                    "samples_found": len(all_features),
                    "minimum_required": 50}

        logger.info(f"Built {len(all_features)} training samples")

    finally:
        conn.close()

    # Convert to numpy/pandas
    X = pd.DataFrame(all_features, columns=FEATURE_NAMES)
    y = np.array(all_targets, dtype=float)

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Train XGBoost
    model = xgb.XGBRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        random_state=42,
        objective="reg:squarederror",
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    # Evaluate
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)

    metrics = {
        "train_mae": round(float(mean_absolute_error(y_train, train_pred)), 3),
        "test_mae": round(float(mean_absolute_error(y_test, test_pred)), 3),
        "train_rmse": round(float(np.sqrt(mean_squared_error(y_train, train_pred))), 3),
        "test_rmse": round(float(np.sqrt(mean_squared_error(y_test, test_pred))), 3),
        "training_samples": len(X_train),
        "test_samples": len(X_test),
        "races_used": len(processed_races),
        "features": FEATURE_NAMES,
    }

    # Feature importance
    importance = dict(zip(FEATURE_NAMES, model.feature_importances_.tolist()))
    metrics["feature_importance"] = {
        k: round(v, 4) for k, v in
        sorted(importance.items(), key=lambda x: x[1], reverse=True)
    }

    # Save model
    model_path = PathManager.MODELS_DIR / "handicapper.json"
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(model_path))
    logger.info(f"Model saved to {model_path}")

    # Save metrics
    metrics_path = PathManager.MODELS_DIR / "training_metrics.json"
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    logger.info(f"Training complete. Test MAE: {metrics['test_mae']}")
    return metrics
