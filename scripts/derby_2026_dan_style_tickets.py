"""'Dan-Style' cascading exotics ticket generator for the 2026 Derby.

REBUILT 2026-05-01 (v3) after DRF PPs PDF revealed TFUS Early/Late pace figures.
Silent Tactic (PP13) and Fulleffort (PP20) scratched; Great White (PP21) and
Ocelli (PP22) drew in via auxiliary gate.

PACE OVERLAY APPLIED: 6 pacesetters (Litmus Test/Potente/So Happy/Pavlovian/
Six Speed/Great White, all TFUS-E 98+) means a SCORCHING pace meltdown.

NEW KEY STRUCTURE: Commandment (PP6) SINGLE KEY on top.
- Commandment is the pace-adjusted model #1 (6-1 → 20.0% Joe, +5.7pp edge)
- Right to Party (PP5, 30-1) is the price overlay (deep closer + PP5 +0.5 bias)
- Renegade (PP1, 4-1) is live but priced as chalk underlay — use in 2nd/3rd slots
- So Happy (PP8) and Further Ado (PP18) DROPPED from top — pace traps

Designed for a $300 bankroll split into three $100 pace-scenario portfolios.
NO win bets — only trifectas and superfectas. Every portfolio cascades from
a tight straight ticket out to a wide insurance wheel so that if the tight
ticket cashes, multiple wider tickets cash too (the 'Superfecta Dan style').

Churchill Downs minimums:
  Trifecta:    $0.50
  Superfecta:  $0.10
"""

from itertools import permutations
from typing import List, Tuple


def _enumerate(slots: List[List[int]]) -> List[Tuple[int, ...]]:
    """Return every distinct ordered combination across the slots."""
    out = []
    for combo in _cartesian(slots):
        if len(set(combo)) == len(combo):
            out.append(combo)
    return out


def _cartesian(slots):
    if not slots:
        yield ()
        return
    head, *rest = slots
    for h in head:
        for tail in _cartesian(rest):
            yield (h,) + tail


def ticket_cost(slots: List[List[int]], base: float) -> Tuple[int, float]:
    combos = _enumerate(slots)
    return len(combos), len(combos) * base


def hits(slots: List[List[int]], result: Tuple[int, ...]) -> bool:
    if len(result) < len(slots):
        return False
    for i, slot in enumerate(slots):
        if result[i] not in slot:
            return False
    if len(set(result[: len(slots)])) != len(slots):
        return False
    return True


def fmt_slots(slots: List[List[int]]) -> str:
    return " / ".join(",".join(str(x) for x in s) for s in slots)


def print_portfolio(name: str, scenario: str, predicted: Tuple[int, ...],
                    tickets: List[dict]) -> float:
    print(f"\n{'=' * 78}")
    print(f"  {name}")
    print(f"  Scenario: {scenario}")
    print(f"  Predicted finish (test result): {' / '.join(map(str, predicted))}")
    print(f"{'=' * 78}")
    print(f"  {'Type':<6} {'Structure':<48} {'Base':<6} {'Combos':<7} {'Cost':<7} Cash?")
    total = 0.0
    cashes = 0
    for t in tickets:
        n, cost = ticket_cost(t["slots"], t["base"])
        cashed = hits(t["slots"], predicted)
        total += cost
        if cashed:
            cashes += 1
        flag = "[CASH]" if cashed else "."
        print(f"  {t['kind']:<6} {fmt_slots(t['slots']):<48} ${t['base']:<5.2f} "
              f"{n:<7d} ${cost:<6.2f} {flag}")
    print(f"  {'-' * 76}")
    print(f"  Portfolio cost: ${total:6.2f}   Tickets that cash on predicted finish: "
          f"{cashes}/{len(tickets)}")
    return total


# Joe's pace-adjusted top after TFUS overlay
JOE_TOP = {
    6:  "Commandment",     # stalker ML 6-1   *** PACE-ADJ #1 (+5.7pp edge) ***
    1:  "Renegade",        # closer  ML 4-1   live but chalk underlay
    9:  "The Puma",        # stalker ML 10-1  late-kick + Castellano (+3.0pp)
    8:  "So Happy",        # PACER   ML 15-1  pace-haircut overlay (+3.0pp)
    5:  "Right to Party",  # closer  ML 30-1  *** TOP PRICE PLAY (+5.3pp) ***
    10: "Wonder Dean",     # closer  ML 30-1  Japan deep-closer bomb (no TFUS)
    19: "Golden Tempo",    # closer  ML 30-1  TFUS-L 113! sneaky DeVaux/Ortiz
    11: "Incredibolt",     # balanced ML 20-1 Mott
    15: "Emerging Market", # stalker ML 15-1  Chad Brown/Prat
    12: "Chief Wallabee",  # press   ML 8-1   pace-trap fade (drop from key)
    18: "Further Ado",     # PRESS   ML 6-1   pace-trap fade (drop from key)
}

# ── PORTFOLIO A — base read: COMMANDMENT KEY (pace-adjusted #1) ────────
# Predicted finish: 6 / 1 / 9 / 5
A_PRED = (6, 1, 9, 5)
A_TICKETS = [
    # Tier 1 — Commandment single key bullseye
    {"kind": "TRI",   "slots": [[6], [1], [9]], "base": 5.00},
    {"kind": "SUPER", "slots": [[6], [1], [9], [5]], "base": 2.00},

    # Tier 2 — Commandment key, top live horses underneath
    {"kind": "TRI",   "slots": [[6], [1, 9, 5, 8], [1, 9, 5, 8]], "base": 1.00},
    {"kind": "SUPER", "slots": [[6], [1, 9, 5, 8], [1, 9, 5, 8], [1, 9, 5, 8, 18]], "base": 0.50},

    # Tier 3 — Wider Commandment key including bombs
    {"kind": "SUPER", "slots": [[6], [1, 9, 5], [1, 9, 5, 8, 18], [1, 9, 5, 8, 18, 10, 19]], "base": 0.10},

    # Tier 4 — Renegade alternate key (the chalk; he IS live, just overpriced)
    {"kind": "TRI",   "slots": [[1], [6, 9, 5, 8], [6, 9, 5, 8, 18]], "base": 0.50},
    {"kind": "SUPER", "slots": [[1], [6, 9, 5, 8], [6, 9, 5, 8, 18], [6, 9, 5, 8, 18, 10, 19]], "base": 0.10},

    # Tier 5 — Right to Party 30-1 bomb on top (deep closer in melt)
    {"kind": "TRI",   "slots": [[5], [6, 1, 9, 8], [6, 1, 9, 8, 18]], "base": 0.50},
    {"kind": "SUPER", "slots": [[5], [6, 1, 9], [6, 1, 9, 8], [6, 1, 9, 8, 18, 10]], "base": 0.10},

    # Tier 6 — wide 10c insurance: top-2 keys, deep wheel
    {"kind": "SUPER", "slots": [[6, 1], [6, 1, 9, 5, 8], [6, 1, 9, 5, 8, 18],
                                 [6, 1, 9, 5, 8, 18, 10, 19, 11, 15]], "base": 0.10},
]

# ── PORTFOLIO B — pace-meltdown jackpot: deep closers sweep ────────────
# All 6 pacesetters cook; Renegade (L116), Right to Party (L108), Golden Tempo
# (L113), Wonder Dean — the late-kick brigade run them down.
# Predicted finish: 1 / 6 / 5 / 19
B_PRED = (1, 6, 5, 19)
B_TICKETS = [
    # Bullseye — Renegade wins (L116 + pace meltdown = unstoppable)
    {"kind": "TRI",   "slots": [[1], [6], [5]], "base": 5.00},
    {"kind": "SUPER", "slots": [[1], [6], [5], [19]], "base": 2.00},

    # Renegade key + closers underneath (the meltdown signature)
    {"kind": "TRI",   "slots": [[1], [6, 5, 9, 19, 10], [6, 5, 9, 19, 10]], "base": 1.00},
    {"kind": "SUPER", "slots": [[1], [6, 5, 9], [6, 5, 9, 19, 10], [6, 5, 9, 19, 10, 8]], "base": 0.50},

    # Right to Party 30-1 wins it (deep closer, PP5 +0.5 bias)
    {"kind": "TRI",   "slots": [[5], [1, 6, 9, 19], [1, 6, 9, 19, 10]], "base": 0.50},
    {"kind": "SUPER", "slots": [[5], [1, 6, 9], [1, 6, 9, 19], [1, 6, 9, 19, 10, 8]], "base": 0.10},

    # Golden Tempo 30-1 bomb (TFUS-L 113! highest in field tied with Renegade)
    {"kind": "TRI",   "slots": [[19], [1, 6, 5, 9], [1, 6, 5, 9, 10]], "base": 0.50},
    {"kind": "SUPER", "slots": [[19], [1, 6, 5], [1, 6, 5, 9], [1, 6, 5, 9, 10, 8]], "base": 0.10},

    # Wonder Dean (Japan, no TFUS but pure deep closer profile)
    {"kind": "SUPER", "slots": [[10], [1, 6, 5], [1, 6, 5, 9, 19], [1, 6, 5, 9, 19, 8]], "base": 0.10},

    # Wide 10c deep-closer wheel (trimmed to keep Portfolio B under $100)
    {"kind": "SUPER", "slots": [[1, 5, 19, 10], [1, 5, 19, 10, 6, 9],
                                 [1, 5, 19, 10, 6, 9, 8], [1, 5, 19, 10, 6, 9, 8]], "base": 0.10},
]

# ── PORTFOLIO C — late-kick stalker tactical: Commandment-Puma 1-2 ──────
# Pace duel cooks the front; Cox + Castellano late-runners overpower.
# Predicted finish: 6 / 9 / 1 / 5
C_PRED = (6, 9, 1, 5)
C_TICKETS = [
    # Bullseye
    {"kind": "TRI",   "slots": [[6], [9], [1]], "base": 5.00},
    {"kind": "SUPER", "slots": [[6], [9], [1], [5]], "base": 2.00},

    # Commandment + Puma 1-2 box (stalker rotation)
    {"kind": "TRI",   "slots": [[6, 9], [6, 9], [1, 5, 8, 18]], "base": 1.00},
    {"kind": "SUPER", "slots": [[6, 9], [6, 9], [1, 5, 8], [1, 5, 8, 18, 19, 10]], "base": 0.50},

    # Puma key (your read + late-kick edge)
    {"kind": "TRI",   "slots": [[9], [6, 1, 5, 8], [6, 1, 5, 8, 18]], "base": 0.50},
    {"kind": "SUPER", "slots": [[9], [6, 1, 5], [6, 1, 5, 8], [6, 1, 5, 8, 18, 10, 19]], "base": 0.10},

    # So Happy still has +3pp adjusted overlay — single small ticket
    {"kind": "SUPER", "slots": [[8], [6, 1, 9], [6, 1, 9, 5], [6, 1, 9, 5, 18, 10]], "base": 0.10},

    # Wide stalker wheel
    {"kind": "SUPER", "slots": [[6, 9], [6, 9, 1, 5, 8], [6, 9, 1, 5, 8, 18],
                                 [6, 9, 1, 5, 8, 18, 10, 19, 11]], "base": 0.10},
]


def horse_legend():
    print("\n  KEY:")
    for pp in sorted(JOE_TOP):
        print(f"    PP{pp:<2}  {JOE_TOP[pp]}")
    print("\n  SCRATCHED: PP13 Silent Tactic (Casse, bruised foot, 4/29)")
    print("             PP20 Fulleffort (Cox, bone chip, 4/30)")
    print("  AE FILLS:  PP21 Great White, PP22 Ocelli (auxiliary gate)")


def main():
    print("=" * 78)
    print("  GRANDPA JOE — DERBY 2026 'SUPERFECTA DAN STYLE' (REBUILT 5/1)")
    print("  Bankroll: $300  |  Three pace-scenario portfolios at $100 each")
    print("  No win bets. Trifectas + Superfectas only.")
    print("  KEY STRUCTURE: Renegade (PP1) + So Happy (PP8) double key on top.")
    print("=" * 78)

    horse_legend()

    a = print_portfolio("PORTFOLIO A — BASE READ (Commandment single-key)",
                        "Pace meltdown; Commandment + Renegade overpower late",
                        A_PRED, A_TICKETS)
    b = print_portfolio("PORTFOLIO B — DEEP-CLOSER MELTDOWN JACKPOT",
                        "All 6 pacesetters cook; Renegade L116 + Right to Party + Golden Tempo run them down",
                        B_PRED, B_TICKETS)
    c = print_portfolio("PORTFOLIO C — STALKER 1-2 (Commandment + Puma)",
                        "Pace cooks the front; Cox + Castellano late-runners 1-2",
                        C_PRED, C_TICKETS)

    print(f"\n{'=' * 78}")
    print(f"  TOTAL BANKROLL DEPLOYED: ${a + b + c:.2f}")
    print(f"{'=' * 78}\n")


if __name__ == "__main__":
    main()
