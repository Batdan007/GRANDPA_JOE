"""
Pydantic models for crawled racing data.

Normalized shape that every site adapter must produce. This lets the ingestion
bridge treat all sources uniformly.
"""

from datetime import date as Date
from typing import List, Optional

from pydantic import BaseModel, Field


class CrawledEntry(BaseModel):
    """A single horse entered in a race."""

    horse_name: str
    jockey_name: Optional[str] = None
    trainer_name: Optional[str] = None
    post_position: Optional[int] = None
    morning_line_odds: Optional[float] = None
    final_odds: Optional[float] = None
    finish_position: Optional[int] = None
    beaten_lengths: Optional[float] = None
    speed_figure: Optional[int] = None
    weight_lbs: Optional[float] = None
    medication: Optional[str] = None
    comment: Optional[str] = None
    payout_win: Optional[float] = None
    payout_place: Optional[float] = None
    payout_show: Optional[float] = None


class CrawledRace(BaseModel):
    """A single race with its entries and (optionally) results."""

    track_code: str
    race_date: str
    race_number: int
    surface: Optional[str] = "dirt"
    distance_furlongs: Optional[float] = None
    track_condition: Optional[str] = None
    race_type: Optional[str] = "allowance"
    grade: Optional[str] = None
    purse: Optional[int] = None
    class_level: Optional[int] = None
    field_size: Optional[int] = None
    entries: List[CrawledEntry] = Field(default_factory=list)

    source_url: Optional[str] = None
    source_site: Optional[str] = None


class CrawledResult(BaseModel):
    """Wrapper returned by each crawler — a batch of races from one page."""

    site: str
    url: str
    fetched_at: str
    races: List[CrawledRace] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)


class CrawlSummary(BaseModel):
    """Summary across all sites after a scheduled run."""

    started_at: str
    finished_at: str
    sites_run: List[str] = Field(default_factory=list)
    races_crawled: int = 0
    entries_crawled: int = 0
    results_ingested: int = 0
    errors: List[str] = Field(default_factory=list)
