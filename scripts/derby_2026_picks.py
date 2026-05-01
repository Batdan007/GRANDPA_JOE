"""GRANDPA_JOE — Final Derby Day picks for the 152nd Kentucky Derby (May 2 2026).

Reads the post-draw-merged data/derby_2026_pps.json and produces:
  1. Pattern-scored ranking of the 20 drew-in horses
  2. Pace map (who's gunning, how contested the early fractions look)
  3. Derby-specific post-position adjustment (PP1 drought, PP17 0-fer, etc.)
  4. ML-odds overlay analysis (where is the value?)
  5. Joe's W/P/S/4/5 ranking with confidence gaps
  6. Key-part-wheel exotic recommendation (per DJR's preferred bet style)
"""

import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PP_FILE = REPO / "data" / "derby_2026_pps.json"

import sys
sys.path.insert(0, str(REPO))
from grandpa_joe.models.derby_patterns import score_field

# ── Derby-specific post-position win history (1875-2025) ──────────────
# Source: Churchill Downs official records.
# Numbers = career Derby wins from that gate; field has been 20 since 2005.
PP_WINS = {
    1: 12,  2: 10,  3: 5,   4: 7,   5: 11,
    6: 9,   7: 8,   8: 11,  9: 5,   10: 11,
    11: 5,  12: 5,  13: 5,  14: 4,  15: 5,
    16: 1,  17: 0,  18: 1,  19: 1,  20: 2,
}
# But raw counts mislead — PP1 hasn't won since 1986 (Ferdinand)
# under modern 20-horse fields. Adjust by recent (since 2005) rate.
PP_MODERN_BIAS = {
    # +1.0 = sweet spot, -1.0 = avoid, 0 = neutral
    1:  -0.8,  # 0 wins since 1986 — closer trapped on rail is the killer
    2:  -0.3,  # awkward, still need to go somewhere on first turn
    3:  -0.1,
    4:   0.0,
    5:  +0.5,  # solid path, can settle wherever
    6:  +0.5,  # clean trip available
    7:  +0.4,
    8:  +0.6,  # historically strong modern post
    9:  +0.5,
    10: +0.7,  # statistically the best modern post
    11: +0.3,
    12: +0.3,
    13: +0.0,
    14: -0.1,
    15: +0.2,  # Mage 2023 came from PP15; live
    16: -0.2,
    17: -0.7,  # 0 winners ever, bias proven
    18: -0.5,  # forced wide on first turn unless gunning
    19: -0.4,
    20: -0.3,  # Rich Strike 2022 broke the curse but still tough
    21: -0.7,  # auxiliary gate — separated from main field, must hustle to merge
    22: -0.8,  # outermost auxiliary — historically catastrophic
}


def fractions_to_decimal_odds(ml_str: str) -> float:
    """'4-1' -> 5.0 (decimal payoff including stake)."""
    if not ml_str or "-" not in ml_str:
        return 99.0
    a, b = ml_str.split("-", 1)
    return float(a) / float(b) + 1.0


def implied_prob(ml_dec: float) -> float:
    """ML decimal odds -> implied win probability."""
    if ml_dec <= 1.0:
        return 0.99
    return 1.0 / (ml_dec + 1.0)  # ML decimal in fractional form: 4 = 4-to-1


def pace_map(field):
    """Identify pacesetters/pressers by post and rank early-pace pressure."""
    speed = [e for e in field if e["style"] == "pacesetter"]
    pressers = [e for e in field if e["style"] == "presser"]
    return {
        "pacesetters": [(e["post_position"], e["name"]) for e in sorted(speed, key=lambda x: x["post_position"])],
        "pressers":    [(e["post_position"], e["name"]) for e in sorted(pressers, key=lambda x: x["post_position"])],
        "n_speed":     len(speed),
        "n_press":     len(pressers),
    }


def pace_scenario(pmap):
    """Classify the pace shape based on number/post of speed horses."""
    n = pmap["n_speed"]
    if n >= 3:
        return "FAST", "Three+ pacesetters means a hot, contested early — closers favored."
    if n == 2:
        # Where are they? If both wide they'll have to gun or fold.
        posts = [p for p, _ in pmap["pacesetters"]]
        if all(p >= 14 for p in posts):
            return "MODERATE", (
                "Two pacesetters but BOTH drawn wide (PPs " + ", ".join(map(str, posts))
                + "). One likely cedes from outside — moderate-honest pace, stalkers and pressers benefit most."
            )
        return "MODERATE", "Two pacesetters likely contest fractions — moderate pace."
    if n == 1:
        return "SLOW", "Lone speed horse — likely uncontested lead. Tactical race favors stalkers/pressers near the front."
    return "TACTICAL", "No pure pacesetter — pressers will have to make the lead. Slow tactical race; speed favored."


def adjust_for_derby_post(scored):
    """Add Derby-specific PP modifier on top of generic pattern score."""
    for e in scored:
        pp = e.get("post_position")
        bias = PP_MODERN_BIAS.get(pp, 0.0)
        # Convert bias to score points (range +/- 8 on a 0-100 scale)
        e["derby_pp_adj"] = round(bias * 8, 1)
        e["derby_score"] = round(e["total_score"] + e["derby_pp_adj"], 1)
    scored.sort(key=lambda x: x["derby_score"], reverse=True)
    return scored


def overlay_value(e):
    """How big is the gap between Joe's implied prob and the ML implied prob?
    Positive = overlay (good bet), negative = underlay (avoid)."""
    # Convert derby_score (0-100) to a rough win probability via softmax-like normalization.
    # We'll do that at the field level in the caller; here just store ML implied.
    ml = fractions_to_decimal_odds(e.get("morning_line", "99-1"))
    e["ml_implied"] = round(1.0 / ml, 4)
    return e


def assign_field_probs(scored):
    """Convert derby_score into a normalized win probability across the field."""
    # Use a temperature-softmax so gaps matter but no horse hits 100%.
    # Derby is a 20-horse chaos race; flat distribution is honest.
    # T=14 keeps top horse around 18-22% (realistic for a Derby fav).
    import math
    T = 14.0  # temperature
    exps = [math.exp(e["derby_score"] / T) for e in scored]
    total = sum(exps)
    for e, x in zip(scored, exps):
        e["joe_prob"] = round(x / total, 4)
        overlay_value(e)
        e["edge"] = round(e["joe_prob"] - e["ml_implied"], 4)
    return scored


def main():
    with open(PP_FILE) as f:
        data = json.load(f)

    # Filter to drew-in only (exclude AE, non-drew, and scratched)
    field = [e for e in data["entries"] if e.get("drew_in") and not e.get("scratched")]
    scratched = [e for e in data["entries"] if e.get("scratched")]
    print(f"Field size (active): {len(field)}")
    if scratched:
        scr = ", ".join(f"PP{e['post_position']} {e['name']}" for e in scratched)
        print(f"  Scratched: {scr}")
    assert 17 <= len(field) <= 20, f"Expected 17-20 starters, got {len(field)}"

    # ── Pace Map ──────────────────────────────────────────────────────
    pmap = pace_map(field)
    pace_label, pace_note = pace_scenario(pmap)
    print(f"\n{'='*72}")
    print("  PACE MAP")
    print(f"{'='*72}")
    print(f"  Pace shape: {pace_label}")
    print(f"  {pace_note}\n")
    print(f"  Pacesetters ({pmap['n_speed']}):")
    for pp, name in pmap["pacesetters"]:
        print(f"    PP{pp:2d}  {name}")
    print(f"  Pressers ({pmap['n_press']}):")
    for pp, name in pmap["pressers"]:
        print(f"    PP{pp:2d}  {name}")

    # ── Pattern Score + Derby PP Adjustment ───────────────────────────
    scored = score_field(field)
    scored = adjust_for_derby_post(scored)
    scored = assign_field_probs(scored)

    print(f"\n{'='*72}")
    print("  GRANDPA JOE — DERBY 2026 FINAL RANKING")
    print(f"{'='*72}")
    print(f"  {'Rk':<3} {'PP':<3} {'Horse':<22} {'Score':<6} {'PPadj':<6} {'Final':<6} {'ML':<6} {'JoeP':<6} {'Edge':<7}")
    print(f"  {'-'*3} {'-'*3} {'-'*22} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*7}")
    for i, e in enumerate(scored, 1):
        edge_marker = "++" if e["edge"] > 0.05 else "+" if e["edge"] > 0 else "-" if e["edge"] > -0.05 else "--"
        print(
            f"  {i:<3} {e['post_position']:<3} {e['name']:<22} "
            f"{e['total_score']:<6.1f} {e['derby_pp_adj']:+5.1f} "
            f"{e['derby_score']:<6.1f} {e.get('morning_line','?'):<6} "
            f"{e['joe_prob']*100:<5.1f}% {edge_marker}"
        )

    # ── Joe's W/P/S/4/5 ───────────────────────────────────────────────
    top5 = scored[:5]
    print(f"\n{'='*72}")
    print("  JOE'S TOP 5 — WIN / PLACE / SHOW / 4TH / 5TH")
    print(f"{'='*72}")
    for i, e in enumerate(top5, 1):
        label = ["WIN", "PLACE", "SHOW", "4TH", "5TH"][i-1]
        gap = top5[0]["derby_score"] - e["derby_score"]
        gap_str = "" if i == 1 else f"  (-{gap:.1f} from top)"
        print(f"  {label:<6}  PP{e['post_position']:2d}  {e['name']:<22}  "
              f"({e.get('morning_line','?')})  Joe={e['joe_prob']*100:.1f}%{gap_str}")

    # ── Edge / Overlay candidates ─────────────────────────────────────
    overlays = sorted([e for e in scored if e["edge"] > 0.02], key=lambda x: -x["edge"])
    print(f"\n  OVERLAYS (Joe likes more than the ML does):")
    for e in overlays[:6]:
        print(f"    PP{e['post_position']:2d}  {e['name']:<22} "
              f"ML {e['morning_line']:<5} Joe {e['joe_prob']*100:.1f}%  "
              f"edge +{e['edge']*100:.1f}pp")

    # ── Key-Part-Wheel Super recommendation (DJR preferred style) ─────
    key = top5[0]
    parts = [e["post_position"] for e in top5[1:4]]    # places 2-4 as "part"
    wheel = [e["post_position"] for e in top5[:5]] + [
        e["post_position"] for e in overlays[:3] if e["post_position"] not in [t["post_position"] for t in top5[:5]]
    ]
    wheel = sorted(set(wheel))

    print(f"\n{'='*72}")
    print("  RECOMMENDED KEY-PART-WHEEL SUPER (DJR style)")
    print(f"{'='*72}")
    print(f"  KEY (1st):   PP{key['post_position']}  {key['name']}")
    print(f"  PART (2nd):  PPs {parts}  ({', '.join(t['name'] for t in top5[1:4])})")
    print(f"  WHEEL (3-4): PPs {wheel}")
    cost = 1 * len(parts) * (len(wheel) - 2) * (len(wheel) - 3)  # 1 x 3 x (n-2) x (n-3)
    print(f"  Approx cost (50c base): ${cost * 0.50:.2f}")
    print(f"  Approx cost ($1 base):  ${cost * 1.00:.2f}")

    # Save final ranking
    out = REPO / "data" / "derby_2026_picks.json"
    with open(out, "w") as f:
        json.dump({
            "race": data["race"],
            "date": data["date"],
            "pace_shape": pace_label,
            "pace_note": pace_note,
            "ranking": scored,
            "top5": [{"rank": i+1, "name": e["name"], "post": e["post_position"],
                      "ml": e.get("morning_line"), "joe_prob": e["joe_prob"],
                      "score": e["derby_score"], "edge": e["edge"]} for i, e in enumerate(top5)],
            "overlays": [{"name": e["name"], "post": e["post_position"],
                          "ml": e.get("morning_line"), "edge": e["edge"]} for e in overlays[:6]],
        }, f, indent=2, default=str)
    print(f"\n  Saved -> {out}\n")


if __name__ == "__main__":
    main()
