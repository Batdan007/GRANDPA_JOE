"""Manual fix for R13 (Kentucky Oaks) — the parser missed Zany due to a
page-boundary issue. Move her from R12 to R13 and re-rank. Also remove
horses from R13 that are confirmed scratched (Bottle of Rouge, My Miss Mo,
Bella Ballerina, Nycon).
"""
import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PICKS = REPO / "data" / "oaks_day_picks.json"

SCRATCHED_OAKS = {
    "bottle of rouge",
    "my miss mo",
    "bella ballerina",
    "nycon",
}


def main():
    with open(PICKS) as f:
        d = json.load(f)
    races = {r["race"]: r for r in d["races"]}

    # Find Zany in R12 and move to R13
    r12 = races.get(12)
    r13 = races.get(13)
    if not r12 or not r13:
        print("[ERR] R12 or R13 missing")
        return

    zany = next((h for h in r12["horses"] if h["name"].lower() == "zany"), None)
    if zany:
        r12["horses"] = [h for h in r12["horses"] if h["name"].lower() != "zany"]
        r13["horses"].append(zany)
        print(f"  Moved Zany from R12 -> R13")

    # Filter scratched horses out of R13
    before = len(r13["horses"])
    r13["horses"] = [h for h in r13["horses"]
                     if h["name"].lower().strip() not in SCRATCHED_OAKS]
    print(f"  Filtered {before - len(r13['horses'])} scratched horse(s) from R13")

    # Re-rank both R12 and R13 with same scoring
    for r in (r12, r13):
        n_pace = sum(1 for h in r["horses"] if h.get("style") == "pacesetter"
                     or (h.get("tfus_early") and h["tfus_early"] >= 100))
        for h in r["horses"]:
            late = h.get("tfus_late", 90) or 90
            early = h.get("tfus_early", 90) or 90
            base = late * 1.0 + max(0, 95 - early) * 0.3
            # pace factor
            if early >= 105 and n_pace >= 3:
                pf = 0.55
            elif early >= 95 and n_pace >= 3:
                pf = 0.85
            elif early <= 80 and late >= 108:
                pf = 1.50
            elif late >= 104 and early <= 94:
                pf = 1.30
            else:
                pf = 1.00
            h["pace_factor"] = pf
            h["score"] = base * pf
        r["horses"].sort(key=lambda h: -h["score"])
        if len(r["horses"]) >= 2:
            r["gap_1_2"] = r["horses"][0]["score"] - r["horses"][1]["score"]
            r["single_confidence"] = ("STRONG" if r["gap_1_2"] >= 15
                                       else "MEDIUM" if r["gap_1_2"] >= 8
                                       else "WIDE")
        r["n_pacesetters"] = n_pace
        r["pace_shape"] = ("FAST" if n_pace >= 3
                           else "MOD" if n_pace == 2
                           else "SLOW/TAC")

    # Tag R13 as the Oaks
    r13["name"] = "KENTUCKY OAKS (G1)"
    r13["post_time"] = "5:51 PM ET"

    with open(PICKS, "w") as f:
        json.dump(d, f, indent=2, default=str)

    print("\n  R13 - KENTUCKY OAKS top picks (post-fix):")
    for i, h in enumerate(r13["horses"][:6], 1):
        print(f"    {i}. PP{h['post']:>2} {h['name']:25s} ML={h['ml']:5s} "
              f"E{h.get('tfus_early', '—')}/L{h.get('tfus_late', '—')} "
              f"score={h['score']:.1f}")
    print(f"\n  Confidence: {r13['single_confidence']}  "
          f"(gap {r13.get('gap_1_2', 0):.1f})")


if __name__ == "__main__":
    main()
