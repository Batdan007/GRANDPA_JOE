"""Tests for Kelly criterion bet sizing."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from grandpa_joe.models.kelly import (
    kelly_fraction, fractional_kelly, morning_line_to_decimal, suggest_bets,
)


def test_kelly_no_edge():
    """No edge should return 0."""
    # 50% chance at even money = no edge
    assert kelly_fraction(0.5, 2.0) == 0.0
    # 10% chance at 5-1 = negative edge
    assert kelly_fraction(0.10, 6.0) == 0.0
    print("PASS: kelly_no_edge")


def test_kelly_positive_edge():
    """Positive edge should return a fraction."""
    # 60% chance at even money (2.0 decimal) = 20% edge
    kf = kelly_fraction(0.6, 2.0)
    assert 0.15 < kf < 0.25  # should be ~0.20
    print("PASS: kelly_positive_edge")


def test_kelly_big_longshot():
    """Longshot with real edge."""
    # 5% chance at 30-1 (31.0 decimal)
    kf = kelly_fraction(0.05, 31.0)
    assert kf > 0  # 0.05 * 31 = 1.55 > 1, so there's edge
    print("PASS: kelly_big_longshot")


def test_fractional_kelly():
    """Quarter Kelly should be 25% of full Kelly."""
    full = kelly_fraction(0.6, 2.0)
    quarter = fractional_kelly(0.6, 2.0, 0.25)
    assert abs(quarter - full * 0.25) < 0.001
    print("PASS: fractional_kelly")


def test_morning_line_conversion():
    """5-1 should become 6.0 decimal."""
    assert morning_line_to_decimal(5.0) == 6.0
    assert morning_line_to_decimal(1.0) == 2.0  # even money
    assert morning_line_to_decimal(0.5) == 1.5   # 1-2
    print("PASS: morning_line_conversion")


def test_suggest_bets():
    """Bet suggestions with edge."""
    rankings = [
        {"horse_name": "FastHorse", "post_position": 3,
         "win_probability": 0.35, "morning_line_odds": 3.0, "confidence": 0.6},
        {"horse_name": "SlowHorse", "post_position": 7,
         "win_probability": 0.05, "morning_line_odds": 20.0, "confidence": 0.3},
    ]
    suggestions = suggest_bets(rankings, bankroll=200, kelly_frac=0.25)
    assert len(suggestions) > 0
    # All amounts should be positive and <= 10% of bankroll
    for s in suggestions:
        assert s.suggested_amount > 0
        assert s.suggested_amount <= 20  # 10% of 200
    print("PASS: suggest_bets")


def test_no_bets_when_no_edge():
    """No suggestions when no edge exists."""
    rankings = [
        {"horse_name": "NoEdge", "post_position": 1,
         "win_probability": 0.10, "morning_line_odds": 5.0, "confidence": 0.3},
    ]
    suggestions = suggest_bets(rankings, bankroll=100, kelly_frac=0.25, min_edge=0.10)
    # 10% * 6.0 = 0.6, edge = -0.4, no bet
    assert len(suggestions) == 0
    print("PASS: no_bets_when_no_edge")


if __name__ == "__main__":
    test_kelly_no_edge()
    test_kelly_positive_edge()
    test_kelly_big_longshot()
    test_fractional_kelly()
    test_morning_line_conversion()
    test_suggest_bets()
    test_no_bets_when_no_edge()
    print("\nAll Kelly tests passed!")
