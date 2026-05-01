"""Pace overlay analysis — applies TFUS Early/Late penalty and bonus on top
of Joe's pattern model output, since the pattern model doesn't see TFUS pace.

In a FAST-pace Derby (6+ pacesetters), the unadjusted model overrates horses
with high TFUS Early because their Beyer/G1 form was earned on tracks where
they had clear leads. In a multi-pacesetter scenario their pace fig is a
liability, not an asset.
"""
import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PICKS = REPO / "data" / "derby_2026_picks.json"
PPS = REPO / "data" / "derby_2026_pps.json"


def pace_factor(early, late):
    """Multiplier on Joe's win prob given TFUS pace + FAST pace shape.

    In a FAST pace shape:
      - High-Early horses (>=105) get HURT  -> 0.55x
      - Mid-Early (95-104) modest hit       -> 0.85x
      - Stalkers (Early 85-94)              -> 1.00x baseline
      - Late-kick (Late >=104, Early <=94)  -> 1.30x bonus
      - Deep closers (Late >=108, Early<=80) -> 1.50x bonus
    """
    if early is None or late is None:
        return 1.0  # Japan/UAE shippers unknown
    if early >= 105:
        return 0.55
    if early >= 95:
        return 0.85
    if early <= 80 and late >= 108:
        return 1.50
    if late >= 104 and early <= 94:
        return 1.30
    return 1.00


def main():
    with open(PICKS) as f:
        picks = json.load(f)
    with open(PPS) as f:
        pps = json.load(f)

    # Build PP -> (tfus_early, tfus_late)
    tfus = {}
    for e in pps["entries"]:
        if not e.get("drew_in") or e.get("scratched"):
            continue
        tfus[e["post_position"]] = (e.get("tfus_early"), e.get("tfus_late"))

    rows = []
    for r in picks["ranking"]:
        pp = r["post_position"]
        e_, l_ = tfus.get(pp, (None, None))
        f = pace_factor(e_, l_)
        adj = r["joe_prob"] * f
        rows.append({
            "pp": pp,
            "name": r["name"],
            "ml": r.get("morning_line"),
            "ml_implied": r["ml_implied"],
            "joe_prob_raw": r["joe_prob"],
            "joe_prob_adj": adj,
            "tfus_early": e_,
            "tfus_late": l_,
            "factor": f,
            "edge_raw": r["edge"],
            "edge_adj": adj - r["ml_implied"],
        })

    # Renormalize after pace adjustment so probabilities sum to ~1
    total = sum(r["joe_prob_adj"] for r in rows)
    for r in rows:
        r["joe_prob_adj"] = r["joe_prob_adj"] / total
        r["edge_adj"] = r["joe_prob_adj"] - r["ml_implied"]

    # Sort by adjusted prob
    rows.sort(key=lambda x: -x["joe_prob_adj"])

    print("=" * 95)
    print("  PACE-ADJUSTED RANKINGS  (TFUS overlay on top of Joe's pattern model)")
    print("  Pace shape: FAST (6 pacesetters, >7 horses with TFUS-Early >= 95)")
    print("=" * 95)
    print(f"  {'Rk':<3} {'PP':<3} {'Horse':<22} {'ML':<6} {'TFUS-E':<7} {'TFUS-L':<7} "
          f"{'×':<5} {'JoeRaw':<7} {'JoeAdj':<7} {'EdgeAdj':<8}")
    print(f"  {'-'*3} {'-'*3} {'-'*22} {'-'*6} {'-'*7} {'-'*7} {'-'*5} {'-'*7} {'-'*7} {'-'*8}")
    for i, r in enumerate(rows[:15], 1):
        e_disp = str(r["tfus_early"]) if r["tfus_early"] else "—"
        l_disp = str(r["tfus_late"]) if r["tfus_late"] else "—"
        flag = ("++" if r["edge_adj"] > 0.05 else "+" if r["edge_adj"] > 0
                else "-" if r["edge_adj"] > -0.05 else "--")
        print(f"  {i:<3} {r['pp']:<3} {r['name']:<22} {r['ml']:<6} "
              f"{e_disp:<7} {l_disp:<7} {r['factor']:<4.2f}x "
              f"{r['joe_prob_raw']*100:<5.1f}% {r['joe_prob_adj']*100:<5.1f}% "
              f"{flag} {r['edge_adj']*100:+.1f}pp")

    print()
    print("  TOP OVERLAYS (Joe-adjusted vs market):")
    overlays = [r for r in rows if r["edge_adj"] > 0.02]
    overlays.sort(key=lambda x: -x["edge_adj"])
    for r in overlays[:6]:
        print(f"    PP{r['pp']:2d}  {r['name']:<22}  ML {r['ml']:<5}  "
              f"Joe {r['joe_prob_adj']*100:.1f}%   edge +{r['edge_adj']*100:.1f}pp")

    print()
    print("  TRAPS (raw model said overlay, but pace says NO):")
    traps = [r for r in rows if r["edge_raw"] > 0.03 and r["edge_adj"] < r["edge_raw"] - 0.05]
    traps.sort(key=lambda x: x["edge_adj"])
    for r in traps[:5]:
        print(f"    PP{r['pp']:2d}  {r['name']:<22}  TFUS-E {r['tfus_early']}  "
              f"raw edge {r['edge_raw']*100:+.1f}pp -> adj {r['edge_adj']*100:+.1f}pp")


if __name__ == "__main__":
    main()
