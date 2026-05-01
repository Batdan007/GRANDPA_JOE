"""Run Derby 2026 picks under both AE-fills-in scenarios.

After Silent Tactic (PP13) scratched, posts 14-20 shifted inward to 13-19,
and an AE will fill PP20. The two AEs are Chip Honcho (Asmussen, pacesetter)
and Iron Honor (Chad Brown, presser). Each shifts the pace shape differently.
"""
import json
import sys
from copy import deepcopy
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PP_FILE = REPO / "data" / "derby_2026_pps.json"
sys.path.insert(0, str(REPO))

from grandpa_joe.models.derby_patterns import score_field
from scripts.derby_2026_picks import (
    pace_map,
    pace_scenario,
    adjust_for_derby_post,
    assign_field_probs,
)


def run_scenario(label: str, ae_name: str):
    with open(PP_FILE) as f:
        data = json.load(f)

    # Active = drew_in and not scratched
    field = [deepcopy(e) for e in data["entries"]
             if e.get("drew_in") and not e.get("scratched")]

    # Bring the named AE in at PP20
    for e in data["entries"]:
        if e["name"] == ae_name:
            ae = deepcopy(e)
            ae["post_position"] = 20
            ae["drew_in"] = True
            ae["ae_filled"] = True
            field.append(ae)
            break

    print("\n" + "=" * 78)
    print(f"  SCENARIO: {label}")
    print(f"  AE drawing in at PP20: {ae_name}")
    print("=" * 78)
    assert len(field) == 20, f"expected 20, got {len(field)}"

    pmap = pace_map(field)
    pace_label, pace_note = pace_scenario(pmap)
    print(f"  Pace: {pace_label}  -- {pace_note}")
    print(f"  Pacesetters: " + ", ".join(f"PP{p} {n}" for p, n in pmap["pacesetters"]))
    print(f"  Pressers:    " + ", ".join(f"PP{p} {n}" for p, n in pmap["pressers"]))

    scored = score_field(field)
    scored = adjust_for_derby_post(scored)
    scored = assign_field_probs(scored)

    print(f"\n  {'Rk':<3} {'PP':<3} {'Horse':<22} {'ML':<6} {'JoeP':<6} {'Edge':<6}")
    print(f"  {'-'*3} {'-'*3} {'-'*22} {'-'*6} {'-'*6} {'-'*6}")
    for i, e in enumerate(scored[:10], 1):
        edge = e["edge"]
        flag = "++" if edge > 0.05 else "+" if edge > 0 else "-" if edge > -0.05 else "--"
        print(f"  {i:<3} {e['post_position']:<3} {e['name']:<22} "
              f"{e.get('morning_line','?'):<6} {e['joe_prob']*100:<5.1f}% {flag}")

    overlays = sorted([e for e in scored if e["edge"] > 0.02], key=lambda x: -x["edge"])
    print(f"\n  OVERLAYS:")
    for e in overlays[:5]:
        print(f"    PP{e['post_position']:2d}  {e['name']:<22} "
              f"ML {e.get('morning_line','?'):<5} edge +{e['edge']*100:.1f}pp")
    return scored


if __name__ == "__main__":
    s1 = run_scenario("CHIP HONCHO IN  (Asmussen pacesetter -> 3 speeds = FAST pace)",
                      "Chip Honcho")
    s2 = run_scenario("IRON HONOR IN  (Chad Brown presser -> stays MODERATE)",
                      "Iron Honor")
