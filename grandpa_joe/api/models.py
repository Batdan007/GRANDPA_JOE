"""
Pydantic request/response models for the GRANDPA_JOE API.
"""

from pydantic import BaseModel, Field
from typing import List, Optional


# ============================================================================
# Requests
# ============================================================================

class HandicapRequest(BaseModel):
    race_id: int
    bankroll: Optional[float] = Field(default=100.0, ge=0)
    kelly_fraction: Optional[float] = Field(default=0.25, ge=0, le=1)


class IngestRequest(BaseModel):
    filepath: str


class BetRecordRequest(BaseModel):
    race_id: int
    bet_type: str = Field(..., pattern=r"^(win|place|show|exacta|trifecta|daily_double|pick3|pick4|pick6|superfecta)$")
    selections: List[int]
    amount: float = Field(..., gt=0)
    odds_at_bet: Optional[float] = None
    user_id: str = "default"
    notes: Optional[str] = None


class BetResolveRequest(BaseModel):
    result: str = Field(..., pattern=r"^(won|lost|scratched|refunded)$")
    payout: float = Field(default=0, ge=0)


class SearchRequest(BaseModel):
    query: str
    limit: int = Field(default=10, ge=1, le=100)


class ChatRequest(BaseModel):
    message: str
    user_id: str = "default"


# ============================================================================
# Responses
# ============================================================================

class HorseRankingResponse(BaseModel):
    rank: int
    entry_id: int
    horse_name: str
    post_position: int
    win_probability: float
    place_probability: float
    show_probability: float
    confidence: float
    morning_line_odds: float


class BetSuggestionResponse(BaseModel):
    horse_name: str
    post_position: int
    bet_type: str
    selections: List[int]
    win_probability: float
    odds: float
    kelly_fraction: float
    suggested_amount: float
    edge: float
    confidence: float


class HandicapResponse(BaseModel):
    race_id: int
    track_code: str
    race_number: int
    race_date: str
    surface: str
    distance_furlongs: float
    rankings: List[HorseRankingResponse]
    bet_suggestions: List[BetSuggestionResponse]
    model_version: str
    ethics_warnings: List[str] = []


class StatsResponse(BaseModel):
    tracks: int
    horses: int
    jockeys: int
    trainers: int
    races: int
    entries: int
    results: int
    past_performances: int
    predictions: int
    bets: int
    handicapping_patterns: int
    gambling_session_log: int
    net_pnl: float
    bet_win_rate: float


class HealthResponse(BaseModel):
    status: str
    version: str
    brain_connected: bool
    model_loaded: bool
    nexus_available: bool


class ChatResponse(BaseModel):
    message: str
    suggestions: Optional[List[str]] = None
