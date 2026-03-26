"""Tests for RacingBrain."""

import os
import sys
import tempfile

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from grandpa_joe.brain.racing_brain import RacingBrain


def test_brain_init():
    """Brain initializes and creates tables."""
    with tempfile.TemporaryDirectory() as tmpdir:
        brain = RacingBrain(data_dir=tmpdir)
        stats = brain.get_memory_stats()
        assert stats["tracks"] == 0
        assert stats["horses"] == 0
        assert stats["races"] == 0
        print("PASS: brain_init")


def test_get_or_create_track():
    """Track creation and caching."""
    with tempfile.TemporaryDirectory() as tmpdir:
        brain = RacingBrain(data_dir=tmpdir)
        tid1 = brain.get_or_create_track("SAR", "Saratoga")
        tid2 = brain.get_or_create_track("SAR", "Saratoga")
        assert tid1 == tid2
        assert brain.track_cache["SAR"] == tid1
        print("PASS: get_or_create_track")


def test_store_race_and_entry():
    """Store a race with entries."""
    with tempfile.TemporaryDirectory() as tmpdir:
        brain = RacingBrain(data_dir=tmpdir)

        race_id = brain.store_race(
            track_code="CD", race_date="2024-05-04", race_number=12,
            race_name="Kentucky Derby", race_type="stakes", grade="G1",
            surface="dirt", distance_furlongs=10.0, purse=5000000,
            track_condition="fast",
        )
        assert race_id > 0

        entry_id = brain.store_entry(
            race_id, "Mystik Dan",
            jockey_name="Brian Hernandez Jr.",
            trainer_name="Kenny McPeek",
            post_position=3, morning_line_odds=15.0, weight_lbs=126,
        )
        assert entry_id > 0

        race = brain.get_race(race_id)
        assert race is not None
        assert race["race_name"] == "Kentucky Derby"
        assert len(race["entries"]) == 1
        assert race["entries"][0]["horse_name"] == "Mystik Dan"
        print("PASS: store_race_and_entry")


def test_store_result():
    """Store and retrieve results."""
    with tempfile.TemporaryDirectory() as tmpdir:
        brain = RacingBrain(data_dir=tmpdir)
        race_id = brain.store_race("SAR", "2024-08-01", 1, surface="turf",
                                    distance_furlongs=8.0)
        entry_id = brain.store_entry(race_id, "Test Horse", post_position=1)
        brain.store_result(entry_id, finish_position=1, speed_figure=95,
                           beaten_lengths=0, final_odds=3.5)

        stats = brain.get_memory_stats()
        assert stats["results"] == 1
        print("PASS: store_result")


def test_past_performances():
    """Store and retrieve past performances."""
    with tempfile.TemporaryDirectory() as tmpdir:
        brain = RacingBrain(data_dir=tmpdir)
        brain.store_past_performance(
            "Speed Demon", "2024-07-01", "SAR",
            surface="dirt", distance_furlongs=6.0, track_condition="fast",
            finish_position=2, speed_figure=88, field_size=10,
        )
        brain.store_past_performance(
            "Speed Demon", "2024-08-01", "SAR",
            surface="dirt", distance_furlongs=6.0, track_condition="fast",
            finish_position=1, speed_figure=92, field_size=8,
        )

        horse_id = brain.horse_cache["Speed Demon"]
        pps = brain.get_horse_pps(horse_id)
        assert len(pps) == 2
        assert pps[0]["speed_figure"] == 92  # most recent first
        print("PASS: past_performances")


def test_bets_and_pnl():
    """Bet recording and P&L tracking."""
    with tempfile.TemporaryDirectory() as tmpdir:
        brain = RacingBrain(data_dir=tmpdir)
        race_id = brain.store_race("GP", "2024-01-01", 1)

        bet1 = brain.store_bet(race_id, "win", [3], 10.0)
        bet2 = brain.store_bet(race_id, "exacta", [3, 7], 5.0)

        brain.resolve_bet(bet1, "won", 35.0)
        brain.resolve_bet(bet2, "lost", 0)

        stats = brain.get_memory_stats()
        assert stats["bets"] == 2
        assert stats["net_pnl"] == 20.0  # 35 - 15 = 20
        assert stats["bet_win_rate"] == 50.0
        print("PASS: bets_and_pnl")


def test_handicapping_patterns():
    """Pattern storage and retrieval."""
    with tempfile.TemporaryDirectory() as tmpdir:
        brain = RacingBrain(data_dir=tmpdir)
        brain.store_pattern(
            "track_bias", "SAR_dirt_inner_rail",
            {"description": "Inner rail advantage on dirt sprints"},
            confidence=0.72, sample_size=150,
        )

        patterns = brain.get_patterns("track_bias")
        assert len(patterns) == 1
        assert patterns[0]["confidence"] == 0.72
        print("PASS: handicapping_patterns")


def test_export():
    """Brain export to JSON."""
    with tempfile.TemporaryDirectory() as tmpdir:
        brain = RacingBrain(data_dir=tmpdir)
        brain.get_or_create_track("SAR", "Saratoga")
        brain.get_or_create_horse("Test Horse")

        export_path = brain.export_to_json(os.path.join(tmpdir, "export.json"))
        assert os.path.exists(export_path)
        print("PASS: export")


if __name__ == "__main__":
    test_brain_init()
    test_get_or_create_track()
    test_store_race_and_entry()
    test_store_result()
    test_past_performances()
    test_bets_and_pnl()
    test_handicapping_patterns()
    test_export()
    print("\nAll brain tests passed!")
