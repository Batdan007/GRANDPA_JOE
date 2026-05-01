"""Fix Oaks Day picks: filter ALL Friday May 1 scratches across every race.

Source: Churchill Downs scratches-and-changes page, May 1 2026.
Updated comprehensive filter — was 4 horses, now 17 across 8 races.
Also moves Zany from R12 to R13 (parser page-boundary fix).
"""
import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PICKS = REPO / "data" / "oaks_day_picks.json"

# Friday May 1, 2026 scratches (from Churchill Downs official page)
# Format: (race_num, horse_name_lowercase) — race-specific so AE horses
# moved between races aren't accidentally filtered from the right race.
SCRATCHED = {
    (1,  "making daisys"),
    (5,  "hill country"),
    (5,  "drop shot"),
    (5,  "apollo's glory"),
    (5,  "wembley avenue"),
    (6,  "nimah"),
    (6,  "kayla's komet"),
    (6,  "heaven's bolt"),
    (6,  "don't do it lucy"),
    (7,  "buttercream babe"),
    (9,  "disruptor"),
    (10, "pin up betty"),
    (12, "lovely grey"),  # the R12 entry; the Oaks-AE Lovely Grey in R13 is OK
    (13, "my miss mo"),
    (13, "bottle of rouge"),
    (13, "bella ballerina"),
    (13, "nycon"),
}


def is_scratched(race_num: int, name: str) -> bool:
    n = name.lower().strip()
    # Strip trailing asterisks (*) and country tags often present in DRF data
    n = n.rstrip("*").strip()
    n = n.replace("(ire)", "").replace("(gb)", "").replace("(jpn)", "")
    n = n.replace("(arg)", "").replace("(chi)", "").strip()
    return (race_num, n) in SCRATCHED


def rank_with_pace_overlay(horses: list) -> list:
    """Re-rank a race's horses with the same scoring used elsewhere."""
    n_pace = sum(1 for h in horses if h.get("style") == "pacesetter"
                 or (h.get("tfus_early") and h["tfus_early"] >= 100))
    for h in horses:
        late = h.get("tfus_late") or 90
        early = h.get("tfus_early") or 90
        base = late * 1.0 + max(0, 95 - early) * 0.3
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
    horses.sort(key=lambda h: -h["score"])
    return horses, n_pace


def main():
    with open(PICKS) as f:
        d = json.load(f)
    races = {r["race"]: r for r in d["races"]}

    # ── Step 1: Move Zany from R12 to R13 (parser page-boundary bug) ──
    r12 = races.get(12)
    r13 = races.get(13)
    if r12 and r13:
        zany = next((h for h in r12["horses"] if h["name"].lower() == "zany"), None)
        if zany:
            r12["horses"] = [h for h in r12["horses"]
                             if h["name"].lower() != "zany"]
            r13["horses"].append(zany)
            print("  [fix] Moved Zany from R12 -> R13")

    # ── Step 2: Filter scratches across EVERY race ────────────────────
    total_filtered = 0
    for r in d["races"]:
        before = len(r["horses"])
        r["horses"] = [h for h in r["horses"]
                       if not is_scratched(r["race"], h["name"])]
        removed = before - len(r["horses"])
        if removed:
            print(f"  [fix] R{r['race']}: filtered {removed} scratched horse(s)")
            total_filtered += removed

    print(f"  [fix] Total scratched horses removed: {total_filtered}")

    # ── Step 3: Re-rank every race after filtering ────────────────────
    for r in d["races"]:
        if not r["horses"]:
            continue
        r["horses"], n_pace = rank_with_pace_overlay(r["horses"])
        if len(r["horses"]) >= 2:
            r["gap_1_2"] = r["horses"][0]["score"] - r["horses"][1]["score"]
            r["single_confidence"] = ("STRONG" if r["gap_1_2"] >= 15
                                       else "MEDIUM" if r["gap_1_2"] >= 8
                                       else "WIDE")
        else:
            r["single_confidence"] = "UNKNOWN"
        r["n_pacesetters"] = n_pace
        r["pace_shape"] = ("FAST" if n_pace >= 3
                           else "MOD" if n_pace == 2
                           else "SLOW/TAC")

    # Tag R13 as the Oaks
    if 13 in races:
        races[13]["name"] = "KENTUCKY OAKS (G1)"
        races[13]["post_time"] = "5:51 PM ET"

    with open(PICKS, "w") as f:
        json.dump(d, f, indent=2, default=str)

    # ── Print revised top 5 of every race ──────────────────────────────
    print()
    print("=" * 90)
    print("  POST-SCRATCH ORDER OF FINISH")
    print("=" * 90)
    for r in sorted(d["races"], key=lambda x: x["race"]):
        if not r["horses"]:
            continue
        h = r["horses"]
        gap = r.get("gap_1_2", 0)
        print(f"\n  R{r['race']:<2}  ({len(h)} starters, pace={r.get('pace_shape','?')}, "
              f"conf={r.get('single_confidence','?')}, gap={gap:.1f})")
        for i, hh in enumerate(h[:4], 1):
            print(f"    {i}. PP{hh['post']:>2} {hh['name']:25s}  ML={hh.get('ml','—'):5}  "
                  f"score={hh['score']:.1f}")


if __name__ == "__main__":
    main()
