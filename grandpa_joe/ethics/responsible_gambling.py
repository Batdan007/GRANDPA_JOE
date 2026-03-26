"""
Responsible Gambling Guard for GRANDPA_JOE.
Follows ALFRED's JoeDogRule pattern — enforced at every bet interaction.

Grandpa Joe cares about people. The track is fun, but it can also hurt.
This module ensures we never contribute to problem gambling.

National Problem Gambling Helpline: 1-800-522-4700
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


HELPLINE = "1-800-522-4700"
HELPLINE_TEXT = "Text HOME to 741741"
HELPLINE_CHAT = "ncpgambling.org/chat"


class ViolationType(Enum):
    """Types of responsible gambling violations."""
    LOSS_LIMIT_EXCEEDED = "loss_limit"
    SESSION_TIME_EXCEEDED = "session_time"
    LOSS_STREAK_COOLDOWN = "loss_streak"
    BET_SIZE_EXCESSIVE = "bet_size"
    CHASING_LOSSES = "chasing"
    PROBLEM_SIGNALS = "problem_signals"


class BlockLevel(Enum):
    """How strictly to enforce."""
    HARD = "hard"    # Cannot proceed
    SOFT = "soft"    # Warning, can override


@dataclass
class EthicsCheckResult:
    """Result of an ethics check."""
    is_safe: bool
    violations: List[Dict] = field(default_factory=list)
    message: str = ""
    suggestion: Optional[str] = None
    block_level: Optional[BlockLevel] = None

    def add_violation(self, violation_type: ViolationType, detail: str,
                      level: BlockLevel = BlockLevel.SOFT):
        self.violations.append({
            "type": violation_type.value,
            "detail": detail,
            "level": level.value,
        })
        if level == BlockLevel.HARD:
            self.is_safe = False
            self.block_level = BlockLevel.HARD
        elif not self.block_level:
            self.block_level = BlockLevel.SOFT


# Problem gambling signal patterns
PROBLEM_PATTERNS = [
    (r"(?i)\b(need|have|got)\s+to\s+win\s+(\S+\s+)*back", BlockLevel.HARD),
    (r"(?i)\blast\s+(bet|one|wager)\s+i\s+promise", BlockLevel.HARD),
    (r"(?i)\bborrow(ed|ing)?\s+(money|cash)\s+to\s+(bet|gamble|wager)", BlockLevel.HARD),
    (r"(?i)\bcan'?t\s+stop\b", BlockLevel.HARD),
    (r"(?i)\baddicted\b", BlockLevel.HARD),
    (r"(?i)\bchasing\s+(losses|my money)", BlockLevel.HARD),
    (r"(?i)\blost\s+everything\b", BlockLevel.HARD),
    (r"(?i)\bhide\s+(my|the)\s+(betting|gambling|losses)", BlockLevel.HARD),
    (r"(?i)\bbet\s+(the|my)\s+(rent|mortgage|bills)", BlockLevel.HARD),
    (r"(?i)\ball\s+in\b", BlockLevel.SOFT),
    (r"(?i)\bdouble\s+(down|or nothing)\b", BlockLevel.SOFT),
    (r"(?i)\blet\s+it\s+ride\b", BlockLevel.SOFT),
    (r"(?i)\bgo\s+big\s+or\s+go\s+home\b", BlockLevel.SOFT),
    (r"(?i)\bfeeling\s+lucky\b", BlockLevel.SOFT),
]


class ResponsibleGamblingGuard:
    """
    Enforces responsible gambling limits.
    Checked before every bet suggestion and bet recording.
    """

    def __init__(self, config, brain):
        """
        Args:
            config: GamblingLimits dataclass
            brain: RacingBrain instance
        """
        self.config = config
        self.brain = brain
        self._violation_count = 0

    def check_bet(self, bet_amount: float, user_id: str = "default") -> EthicsCheckResult:
        """
        Check if a proposed bet is within responsible limits.
        Called BEFORE every bet suggestion or recording.
        """
        result = EthicsCheckResult(is_safe=True)

        # Check single bet size
        if bet_amount > self.config.max_single_bet:
            result.add_violation(
                ViolationType.BET_SIZE_EXCESSIVE,
                f"${bet_amount:.2f} exceeds max single bet of ${self.config.max_single_bet:.2f}",
                BlockLevel.SOFT
            )

        # Check daily loss limit
        session_stats = self.brain.get_user_session_stats(user_id, days=1)
        daily_loss = session_stats["total_wagered"] - session_stats["total_returned"]
        if daily_loss + bet_amount > self.config.daily_loss_limit:
            result.add_violation(
                ViolationType.LOSS_LIMIT_EXCEEDED,
                f"Daily loss would reach ${daily_loss + bet_amount:.2f} "
                f"(limit: ${self.config.daily_loss_limit:.2f})",
                BlockLevel.HARD
            )

        # Check for loss streak chasing
        recent_bets = self._get_recent_bets(user_id, limit=10)
        loss_streak = self._count_loss_streak(recent_bets)
        if loss_streak >= self.config.cooldown_after_loss_streak:
            result.add_violation(
                ViolationType.LOSS_STREAK_COOLDOWN,
                f"{loss_streak} consecutive losses — take a break",
                BlockLevel.HARD
            )

        # Check for bet escalation after losses (chasing)
        if self._detect_chasing(recent_bets, bet_amount):
            result.add_violation(
                ViolationType.CHASING_LOSSES,
                "Bet size is increasing after losses — classic chasing pattern",
                BlockLevel.SOFT
            )

        # Build message
        if not result.is_safe:
            result.message = self._build_block_message(result.violations)
            result.suggestion = self._get_helpline_message()
        elif result.violations:
            result.message = self._build_warning_message(result.violations)

        return result

    def check_session(self, user_id: str = "default") -> EthicsCheckResult:
        """Check if current session is within time limits."""
        result = EthicsCheckResult(is_safe=True)

        conn = self.brain._connect()
        try:
            active = conn.execute(
                "SELECT * FROM gambling_session_log "
                "WHERE user_id = ? AND session_end IS NULL "
                "ORDER BY session_start DESC LIMIT 1",
                (user_id,)
            ).fetchone()

            if active:
                start = datetime.fromisoformat(active["session_start"])
                elapsed = (datetime.now() - start).total_seconds() / 60

                if elapsed > self.config.session_time_limit_minutes:
                    result.add_violation(
                        ViolationType.SESSION_TIME_EXCEEDED,
                        f"Session running {elapsed:.0f} min "
                        f"(limit: {self.config.session_time_limit_minutes} min)",
                        BlockLevel.SOFT
                    )
                    result.message = self._build_warning_message(result.violations)
        finally:
            conn.close()

        return result

    def check_text(self, text: str) -> EthicsCheckResult:
        """Scan user text for problem gambling signals."""
        result = EthicsCheckResult(is_safe=True)

        for pattern, level in PROBLEM_PATTERNS:
            if re.search(pattern, text):
                result.add_violation(
                    ViolationType.PROBLEM_SIGNALS,
                    f"Detected concerning language pattern",
                    level
                )

        if result.violations:
            hard = any(v["level"] == "hard" for v in result.violations)
            if hard:
                result.message = (
                    "Hey kid, I gotta be straight with you. What you just said "
                    "concerns me. I've seen too many good people hurt at the track.\n\n"
                    f"Please call the National Problem Gambling Helpline: {HELPLINE}\n"
                    f"Or text: {HELPLINE_TEXT}\n"
                    f"Or chat: {HELPLINE_CHAT}\n\n"
                    "There's no shame in asking for help. The bravest thing a person "
                    "can do is admit they need it."
                )
            else:
                result.message = (
                    "Easy there, kid. The track'll always be here tomorrow. "
                    "Let's keep our heads and bet smart, not emotional."
                )

        return result

    def _get_recent_bets(self, user_id: str, limit: int = 10) -> List[Dict]:
        """Get recent bets for a user."""
        conn = self.brain._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM bets WHERE user_id = ? "
                "ORDER BY placed_at DESC LIMIT ?",
                (user_id, limit)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def _count_loss_streak(self, bets: List[Dict]) -> int:
        """Count consecutive losses from most recent."""
        streak = 0
        for bet in bets:
            if bet["result"] == "lost":
                streak += 1
            elif bet["result"] == "won":
                break
            # skip pending/scratched
        return streak

    def _detect_chasing(self, bets: List[Dict], next_amount: float) -> bool:
        """Detect if bet sizes are escalating after losses."""
        lost_bets = [b for b in bets[:5] if b["result"] == "lost"]
        if len(lost_bets) < 2:
            return False

        # Check if next bet is bigger than average of recent losing bets
        avg_loss_bet = sum(b["amount"] for b in lost_bets) / len(lost_bets)
        return next_amount > avg_loss_bet * 1.5

    def _build_block_message(self, violations: List[Dict]) -> str:
        """Build a firm but caring block message."""
        details = "\n".join(f"  - {v['detail']}" for v in violations)
        return (
            f"Son, I can't let you place this bet. Here's why:\n{details}\n\n"
            "I know it's not what you want to hear, but Grandpa Joe looks out "
            "for the people he cares about. Take a walk, get some fresh air, "
            "and come back when you're thinking clearly."
        )

    def _build_warning_message(self, violations: List[Dict]) -> str:
        """Build a gentle warning message."""
        details = "\n".join(f"  - {v['detail']}" for v in violations)
        return (
            f"Just a heads up, kid:\n{details}\n\n"
            "I'm not stopping you, but I want you to think about it. "
            "Smart money is patient money."
        )

    @staticmethod
    def _get_helpline_message() -> str:
        return (
            f"National Problem Gambling Helpline: {HELPLINE}\n"
            f"Text: {HELPLINE_TEXT}\n"
            f"Chat: {HELPLINE_CHAT}"
        )


# Global convenience
_guard = None


def get_gambling_guard(config=None, brain=None) -> Optional[ResponsibleGamblingGuard]:
    """Get or create the global gambling guard."""
    global _guard
    if _guard is None and config and brain:
        _guard = ResponsibleGamblingGuard(config, brain)
    return _guard
