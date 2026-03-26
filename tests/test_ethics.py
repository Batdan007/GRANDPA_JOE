"""Tests for responsible gambling guard."""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from grandpa_joe.brain.racing_brain import RacingBrain
from grandpa_joe.config import GamblingLimits
from grandpa_joe.ethics.responsible_gambling import ResponsibleGamblingGuard


def _make_guard(tmpdir):
    """Create a guard with test brain and config."""
    brain = RacingBrain(data_dir=tmpdir)
    config = GamblingLimits(
        daily_loss_limit=50.0,
        max_single_bet=25.0,
        cooldown_after_loss_streak=3,
    )
    return ResponsibleGamblingGuard(config, brain), brain


def test_normal_bet_passes():
    """A normal bet within limits should pass."""
    with tempfile.TemporaryDirectory() as tmpdir:
        guard, brain = _make_guard(tmpdir)
        result = guard.check_bet(10.0)
        assert result.is_safe
        assert len(result.violations) == 0
        print("PASS: normal_bet_passes")


def test_excessive_single_bet():
    """Bet over max should trigger soft warning."""
    with tempfile.TemporaryDirectory() as tmpdir:
        guard, brain = _make_guard(tmpdir)
        result = guard.check_bet(30.0)
        # Should have a violation but still be "safe" (soft block)
        assert len(result.violations) > 0
        assert result.violations[0]["type"] == "bet_size"
        print("PASS: excessive_single_bet")


def test_problem_gambling_detection_hard():
    """Hard block on problem gambling signals."""
    with tempfile.TemporaryDirectory() as tmpdir:
        guard, brain = _make_guard(tmpdir)
        result = guard.check_text("I need to win this money back")
        assert not result.is_safe or len(result.violations) > 0
        assert "problem_signals" in [v["type"] for v in result.violations]
        print("PASS: problem_gambling_detection_hard")


def test_problem_gambling_detection_soft():
    """Soft warning on mild gambling signals."""
    with tempfile.TemporaryDirectory() as tmpdir:
        guard, brain = _make_guard(tmpdir)
        result = guard.check_text("feeling lucky today, let it ride!")
        assert len(result.violations) > 0
        print("PASS: problem_gambling_detection_soft")


def test_safe_text_passes():
    """Normal text should pass ethics check."""
    with tempfile.TemporaryDirectory() as tmpdir:
        guard, brain = _make_guard(tmpdir)
        result = guard.check_text("What do you think about race 5 at Saratoga?")
        assert result.is_safe
        assert len(result.violations) == 0
        print("PASS: safe_text_passes")


def test_helpline_in_hard_block():
    """Hard blocks should include helpline info."""
    with tempfile.TemporaryDirectory() as tmpdir:
        guard, brain = _make_guard(tmpdir)
        result = guard.check_text("I'm addicted and can't stop betting")
        assert "1-800-522-4700" in (result.message or "")
        print("PASS: helpline_in_hard_block")


if __name__ == "__main__":
    test_normal_bet_passes()
    test_excessive_single_bet()
    test_problem_gambling_detection_hard()
    test_problem_gambling_detection_soft()
    test_safe_text_passes()
    test_helpline_in_hard_block()
    print("\nAll ethics tests passed!")
