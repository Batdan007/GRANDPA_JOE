"""
Racing-specific API routes for GRANDPA_JOE.
"""

import logging
from typing import Optional

from grandpa_joe.api.models import (
    BetRecordRequest, BetResolveRequest, BetSuggestionResponse,
    ChatRequest, ChatResponse, HandicapResponse, HorseRankingResponse,
    SearchRequest,
)

logger = logging.getLogger(__name__)

try:
    from fastapi import APIRouter, HTTPException, Request, UploadFile, File, Query
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

if FASTAPI_AVAILABLE:
    router = APIRouter()

    # ====================================================================
    # RACES
    # ====================================================================

    @router.get("/races/{race_id}")
    async def get_race(race_id: int, request: Request):
        """Get race card with all entries."""
        race = request.app.state.brain.get_race(race_id)
        if not race:
            raise HTTPException(404, f"Race {race_id} not found")
        return race

    @router.get("/races/track/{track_code}")
    async def get_races_by_track(track_code: str, request: Request,
                                  limit: int = Query(default=20, ge=1, le=100)):
        """Get recent races at a track."""
        conn = request.app.state.brain._connect()
        try:
            rows = conn.execute(
                "SELECT r.*, t.code as track_code, t.name as track_name "
                "FROM races r JOIN tracks t ON r.track_id = t.id "
                "WHERE t.code = ? ORDER BY r.race_date DESC, r.race_number "
                "LIMIT ?",
                (track_code.upper(), limit)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ====================================================================
    # HANDICAPPING
    # ====================================================================

    @router.post("/handicap/{race_id}", response_model=HandicapResponse)
    async def handicap_race(race_id: int, request: Request,
                            bankroll: float = Query(default=100.0, ge=0),
                            kelly_fraction: float = Query(default=0.25, ge=0, le=1)):
        """Run the handicapping model on a race and get rankings + bet suggestions."""
        brain = request.app.state.brain
        guard = request.app.state.guard

        race = brain.get_race(race_id)
        if not race:
            raise HTTPException(404, f"Race {race_id} not found")

        # Get rankings
        handicapper = request.app.state.handicapper
        if handicapper:
            rankings = handicapper.predict(race_id)
            handicapper.save_predictions(race_id, rankings)
        else:
            # Morning line fallback
            entries = race.get("entries", [])
            rankings = []
            for i, e in enumerate(sorted(entries, key=lambda x: x.get("morning_line_odds") or 99)):
                rankings.append({
                    "rank": i + 1,
                    "entry_id": e["id"],
                    "horse_name": e["horse_name"],
                    "post_position": e.get("post_position", 0),
                    "win_probability": 1.0 / (i + 2),
                    "place_probability": min(1.0 / (i + 1.5), 0.95),
                    "show_probability": min(1.0 / (i + 1.2), 0.98),
                    "confidence": 0.3,
                    "morning_line_odds": e.get("morning_line_odds") or 10.0,
                })

        # Generate bet suggestions
        ethics_warnings = []
        bet_suggestions = []
        try:
            from grandpa_joe.models.kelly import suggest_bets
            suggestions = suggest_bets(rankings, bankroll, kelly_fraction)

            for s in suggestions:
                # Ethics check on each suggested bet
                check = guard.check_bet(s.suggested_amount)
                if not check.is_safe:
                    ethics_warnings.append(check.message)
                    continue
                if check.violations:
                    ethics_warnings.append(check.message)

                bet_suggestions.append(BetSuggestionResponse(
                    horse_name=s.horse_name,
                    post_position=s.post_position,
                    bet_type=s.bet_type,
                    selections=s.selections,
                    win_probability=s.win_probability,
                    odds=s.odds,
                    kelly_fraction=s.kelly_fraction,
                    suggested_amount=s.suggested_amount,
                    edge=s.edge,
                    confidence=s.confidence,
                ))
        except ImportError:
            pass

        from grandpa_joe.models.handicapper import GrandpaJoeHandicapper

        return HandicapResponse(
            race_id=race_id,
            track_code=race.get("track_code", "UNK"),
            race_number=race.get("race_number", 0),
            race_date=race.get("race_date", ""),
            surface=race.get("surface", "dirt"),
            distance_furlongs=race.get("distance_furlongs", 6.0),
            rankings=[HorseRankingResponse(**r) for r in rankings],
            bet_suggestions=bet_suggestions,
            model_version=GrandpaJoeHandicapper.MODEL_VERSION,
            ethics_warnings=ethics_warnings,
        )

    # ====================================================================
    # BETS
    # ====================================================================

    @router.post("/bets/record")
    async def record_bet(bet: BetRecordRequest, request: Request):
        """Record a placed bet."""
        guard = request.app.state.guard
        brain = request.app.state.brain

        # Ethics check
        check = guard.check_bet(bet.amount, bet.user_id)
        if not check.is_safe:
            return {"status": "blocked", "message": check.message,
                    "suggestion": check.suggestion}

        bet_id = brain.store_bet(
            race_id=bet.race_id,
            bet_type=bet.bet_type,
            selections=bet.selections,
            amount=bet.amount,
            user_id=bet.user_id,
            odds_at_bet=bet.odds_at_bet,
            notes=bet.notes,
        )

        warnings = []
        if check.violations:
            warnings.append(check.message)

        return {"status": "recorded", "bet_id": bet_id, "warnings": warnings}

    @router.post("/bets/{bet_id}/resolve")
    async def resolve_bet(bet_id: int, resolve: BetResolveRequest,
                          request: Request):
        """Resolve a bet (won/lost/scratched)."""
        request.app.state.brain.resolve_bet(bet_id, resolve.result, resolve.payout)
        return {"status": "resolved", "bet_id": bet_id,
                "result": resolve.result, "payout": resolve.payout}

    @router.get("/bets/history")
    async def bet_history(request: Request,
                          user_id: str = Query(default="default"),
                          limit: int = Query(default=50, ge=1, le=500)):
        """Get bet history for a user."""
        conn = request.app.state.brain._connect()
        try:
            rows = conn.execute(
                "SELECT b.*, r.race_date, t.code as track_code, r.race_number "
                "FROM bets b "
                "LEFT JOIN races r ON b.race_id = r.id "
                "LEFT JOIN tracks t ON r.track_id = t.id "
                "WHERE b.user_id = ? ORDER BY b.placed_at DESC LIMIT ?",
                (user_id, limit)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ====================================================================
    # STATS & SEARCH
    # ====================================================================

    @router.get("/stats/track/{track_code}")
    async def track_bias(track_code: str, request: Request,
                         surface: Optional[str] = None,
                         days: int = Query(default=365, ge=1)):
        """Get track bias analysis."""
        return request.app.state.brain.get_track_bias(
            track_code.upper(), surface, days
        )

    @router.get("/stats/horse/{horse_id}")
    async def horse_stats(horse_id: int, request: Request):
        """Get horse past performances."""
        pps = request.app.state.brain.get_horse_pps(horse_id, limit=20)
        if not pps:
            raise HTTPException(404, f"No data for horse {horse_id}")
        return {"horse_id": horse_id, "past_performances": pps}

    @router.post("/search")
    async def search(search: SearchRequest, request: Request):
        """TF-IDF search across the racing brain."""
        results = request.app.state.brain.search(search.query, search.limit)
        return {"query": search.query, "results": results}

    @router.get("/stats/pnl")
    async def pnl_stats(request: Request,
                         user_id: str = Query(default="default"),
                         days: int = Query(default=30)):
        """Get P&L stats for a user."""
        return request.app.state.brain.get_user_session_stats(user_id, days)

    # ====================================================================
    # DATA INGESTION
    # ====================================================================

    @router.post("/ingest")
    async def ingest_csv(file: UploadFile = File(...), request: Request = None):
        """Upload and ingest a CSV file."""
        import tempfile
        from grandpa_joe.brain.ingestion import ingest_csv as do_ingest

        # Save uploaded file to temp location
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv",
                                          mode="wb") as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name

        try:
            result = do_ingest(request.app.state.brain, tmp_path)
            return {"status": "ok", "counts": result}
        finally:
            import os
            os.unlink(tmp_path)

else:
    router = None
