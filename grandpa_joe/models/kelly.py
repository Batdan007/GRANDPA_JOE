"""
Kelly Criterion bet sizing for GRANDPA_JOE.
Conservative fractional Kelly for real-world bankroll management.
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class BetSuggestion:
    """A suggested bet with sizing."""
    horse_name: str
    post_position: int
    bet_type: str          # "win", "place", "show", "exacta", "trifecta"
    selections: list       # [3] for win, [3,7] for exacta, etc.
    win_probability: float
    odds: float            # decimal odds (e.g., 5.0 means 4-1)
    kelly_fraction: float
    suggested_amount: float
    edge: float            # expected edge percentage
    confidence: float


def kelly_fraction(win_probability: float, decimal_odds: float) -> float:
    """
    Full Kelly criterion: optimal fraction of bankroll to wager.

    Args:
        win_probability: estimated probability of winning (0-1)
        decimal_odds: decimal odds (payout per $1 wagered, including stake)
                      e.g., 5.0 means you get $5 back on a $1 bet (4-1)

    Returns:
        Fraction of bankroll to bet (0 if no edge)
    """
    if decimal_odds <= 1 or win_probability <= 0 or win_probability >= 1:
        return 0.0

    # Kelly: f* = (bp - q) / b
    # where b = decimal_odds - 1, p = win_prob, q = 1 - win_prob
    b = decimal_odds - 1.0
    p = win_probability
    q = 1.0 - p

    edge = (b * p) - q
    if edge <= 0:
        return 0.0

    return edge / b


def fractional_kelly(win_probability: float, decimal_odds: float,
                     fraction: float = 0.25) -> float:
    """
    Fractional Kelly for conservative betting.
    Quarter-Kelly (fraction=0.25) is standard for horse racing.
    """
    full = kelly_fraction(win_probability, decimal_odds)
    return full * fraction


def morning_line_to_decimal(ml_odds: float) -> float:
    """Convert morning line odds (e.g., 5.0 for 5-1) to decimal odds."""
    return ml_odds + 1.0


def suggest_bets(rankings: list, bankroll: float,
                 kelly_frac: float = 0.25,
                 min_edge: float = 0.05,
                 max_bet_pct: float = 0.10) -> List[BetSuggestion]:
    """
    Generate bet suggestions from handicapping rankings.

    Args:
        rankings: list of dicts with horse_name, post_position, win_probability,
                  morning_line_odds, confidence
        bankroll: total bankroll
        kelly_frac: Kelly fraction (0.25 = quarter Kelly)
        min_edge: minimum edge to suggest a bet (5% default)
        max_bet_pct: maximum bet as % of bankroll (10% cap)

    Returns:
        List of BetSuggestion sorted by edge
    """
    suggestions = []
    max_bet = bankroll * max_bet_pct

    for r in rankings:
        win_prob = r.get("win_probability", 0)
        ml_odds = r.get("morning_line_odds", 10)
        decimal_odds = morning_line_to_decimal(ml_odds)

        # Calculate edge
        expected_return = win_prob * decimal_odds
        edge = expected_return - 1.0

        if edge < min_edge:
            continue

        # Kelly sizing
        kf = fractional_kelly(win_prob, decimal_odds, kelly_frac)
        bet_amount = min(bankroll * kf, max_bet)

        if bet_amount < 2.0:  # minimum $2 bet
            continue

        # Round to nearest $1
        bet_amount = round(bet_amount)

        suggestions.append(BetSuggestion(
            horse_name=r.get("horse_name", "Unknown"),
            post_position=r.get("post_position", 0),
            bet_type="win",
            selections=[r.get("post_position", 0)],
            win_probability=win_prob,
            odds=decimal_odds,
            kelly_fraction=kf,
            suggested_amount=float(bet_amount),
            edge=edge,
            confidence=r.get("confidence", 0),
        ))

    # Also suggest place/show for top pick if strong
    if suggestions and suggestions[0].win_probability > 0.25:
        top = suggestions[0]
        # Place bet (top 2)
        place_prob = min(top.win_probability * 1.8, 0.95)
        place_odds = top.odds * 0.45  # approximate place odds
        if place_odds > 1:
            place_kf = fractional_kelly(place_prob, place_odds, kelly_frac)
            place_amt = min(bankroll * place_kf, max_bet)
            if place_amt >= 2:
                suggestions.append(BetSuggestion(
                    horse_name=top.horse_name,
                    post_position=top.post_position,
                    bet_type="place",
                    selections=top.selections,
                    win_probability=place_prob,
                    odds=place_odds,
                    kelly_fraction=place_kf,
                    suggested_amount=float(round(place_amt)),
                    edge=(place_prob * place_odds) - 1.0,
                    confidence=top.confidence * 0.9,
                ))

    # Sort by edge (best value first)
    suggestions.sort(key=lambda s: s.edge, reverse=True)
    return suggestions
