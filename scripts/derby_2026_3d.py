"""3D Pace Projection for the 2026 Kentucky Derby.

Uses scraped contender data (Beyer, running style, sire) to simulate
each horse's projected path through the race at 1/4, 1/2, 3/4, stretch,
and finish. Outputs an interactive plotly 3D HTML.

The model assigns position curves based on running style archetypes,
then adjusts by Beyer class so faster horses gain ground late.
"""

import json
import math
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

try:
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# ── Derby 2026 Entries (scraped Apr 19 2026) ──────────────────────────
# Load entries from scraped PP data
import json as _json
_pp_path = REPO / "data" / "derby_2026_pps.json"
if _pp_path.exists():
    with open(_pp_path) as _f:
        _data = _json.load(_f)
    ENTRIES = []
    for e in _data["entries"]:
        # Use best recent SF from PPs if available, else fall back to Beyer from cheat sheet
        best_sf = None
        if e.get("pps"):
            figs = [p["sf"] for p in e["pps"] if p.get("sf") and p["sf"] > 0]
            best_sf = max(figs) if figs else None
        ENTRIES.append({
            "name": e["name"], "sire": e["sire"], "trainer": e["trainer"],
            "jockey": e["jockey"], "beyer": e.get("beyer") or 90,
            "best_sf": best_sf,
            "style": e["style"], "pts": e.get("pts", 0),
            "record": e.get("record", ""),
            "n_starts": len(e.get("pps", [])),
        })
else:
    # Fallback hardcoded entries
    ENTRIES = [
        {"name": "Commandment", "sire": "Into Mischief", "trainer": "Brad Cox", "jockey": "Luis Saez", "beyer": 99, "best_sf": 123, "style": "stalker", "pts": 150, "record": "5: 4-0-0-1", "n_starts": 5},
        {"name": "Further Ado", "sire": "Gun Runner", "trainer": "Brad Cox", "jockey": "John Velazquez", "beyer": 107, "best_sf": 127, "style": "stalker", "pts": 135, "record": "6: 3-1-1-1", "n_starts": 6},
    ]

# ── Style-based position profiles ─────────────────────────────────────
# Each style gets a base position-curve at each call point (1-20 scale).
# These are MEDIAN positions for a 20-horse field.
# Format: {call: base_position}
STYLE_PROFILES = {
    "pacesetter": {"S": 3,  "Q": 2,  "H": 2,  "T": 3,  "STR": 5,  "F": 8},
    "presser":    {"S": 6,  "Q": 5,  "H": 4,  "T": 4,  "STR": 4,  "F": 5},
    "stalker":    {"S": 10, "Q": 9,  "H": 8,  "T": 6,  "STR": 5,  "F": 4},
    "closer":     {"S": 16, "Q": 15, "H": 14, "T": 11, "STR": 7,  "F": 3},
}
CALL_LABELS = ["Start", "Quarter", "Half", "Three-Qtr", "Stretch", "Finish"]
CALL_KEYS = ["S", "Q", "H", "T", "STR", "F"]

STYLE_COLORS = {
    "pacesetter": "#ff4444",
    "presser":    "#ff8800",
    "stalker":    "#44cc44",
    "closer":     "#4488ff",
}


def beyer_adjustment(beyer: int, field_max: int, field_min: int) -> float:
    """Scale Beyer into a position adjustment.

    Higher Beyer → negative adjustment (moves UP in the field).
    Returns adjustment in positions: -3 (elite) to +3 (weak).
    """
    if field_max == field_min:
        return 0
    normalized = (beyer - field_min) / (field_max - field_min)  # 0=worst, 1=best
    return -3 * (normalized - 0.5)  # range: +1.5 (worst) to -1.5 (best)


def project_derby(entries: list) -> list:
    """Generate projected position at each call for every horse."""
    beyers = [e["beyer"] for e in entries if e["beyer"]]
    bmax, bmin = max(beyers), min(beyers)
    field_size = len(entries)

    projections = []
    for horse in entries:
        style = horse["style"]
        profile = STYLE_PROFILES.get(style, STYLE_PROFILES["stalker"])
        beyer = horse.get("beyer", 95)
        adj = beyer_adjustment(beyer, bmax, bmin)

        # Beyer matters more at stretch/finish than at start
        call_weights = {"S": 0.1, "Q": 0.2, "H": 0.4, "T": 0.6, "STR": 0.8, "F": 1.0}

        positions = {}
        for call in CALL_KEYS:
            base = profile[call]
            # Scale base position to field size
            scaled = base * (field_size / 20.0)
            # Apply Beyer adjustment (weighted by call point)
            adjusted = scaled + adj * call_weights[call]
            positions[call] = max(1, min(field_size, round(adjusted, 1)))

        # Compute lengths behind leader (rough: position * 1.5 lengths avg gap)
        lb = {call: max(0, (positions[call] - 1) * 1.2) for call in CALL_KEYS}

        # Estimated cumulative time at each call (1.25 mile Derby)
        # Average Derby fractions: ~23s, ~47s, ~72s, ~97s, ~114s, ~122s
        base_times = {"S": 0, "Q": 23.0, "H": 47.0, "T": 72.0, "STR": 97.0, "F": 122.0}
        times = {call: base_times[call] + lb[call] * 0.17 for call in CALL_KEYS}

        projections.append({
            "name": horse["name"],
            "sire": horse["sire"],
            "trainer": horse["trainer"],
            "jockey": horse["jockey"],
            "beyer": beyer,
            "style": style,
            "positions": positions,
            "lengths_behind": lb,
            "times": times,
        })

    # Sort by projected finish position
    projections.sort(key=lambda p: p["positions"]["F"])
    return projections


def render_3d(projections: list, output: str = "derby_2026_3d.html"):
    if not PLOTLY_AVAILABLE:
        print("ERROR: pip install plotly")
        return

    fig = go.Figure()
    field_size = len(projections)
    x_vals = list(range(len(CALL_KEYS)))

    for i, horse in enumerate(projections):
        color = STYLE_COLORS.get(horse["style"], "#cccccc")
        positions = [horse["positions"][c] for c in CALL_KEYS]
        lb_vals = [horse["lengths_behind"][c] for c in CALL_KEYS]
        times = [horse["times"][c] for c in CALL_KEYS]

        # Z = inverted position (higher = closer to lead)
        z_vals = [field_size + 1 - p for p in positions]

        labels = []
        for j, call in enumerate(CALL_KEYS):
            labels.append(
                f"<b>{horse['name']}</b><br>"
                f"{CALL_LABELS[j]}<br>"
                f"Position: {positions[j]:.0f} of {field_size}<br>"
                f"Lengths behind: {lb_vals[j]:.1f}<br>"
                f"Est. time: {times[j]:.1f}s<br>"
                f"Style: {horse['style']}<br>"
                f"Beyer: {horse['beyer']}<br>"
                f"Sire: {horse['sire']}<br>"
                f"Trainer: {horse['trainer']}<br>"
                f"Jockey: {horse['jockey']}"
            )

        fig.add_trace(go.Scatter3d(
            x=x_vals,
            y=lb_vals,
            z=z_vals,
            mode="lines+markers",
            name=f"{horse['name']} [{horse['style']}] Beyer:{horse['beyer']}",
            line=dict(color=color, width=5),
            marker=dict(size=5, color=color),
            text=labels,
            hoverinfo="text",
        ))

        # Finish label
        fig.add_trace(go.Scatter3d(
            x=[x_vals[-1] + 0.4],
            y=[lb_vals[-1]],
            z=[z_vals[-1]],
            mode="text",
            text=[f"{i+1}. {horse['name']}"],
            textfont=dict(size=9, color=color),
            showlegend=False,
            hoverinfo="skip",
        ))

    # Projected finish order
    finish_order = " > ".join(p["name"] for p in projections[:5])

    fig.update_layout(
        title=dict(
            text=(
                f"<b>GRANDPA JOE — 2026 Kentucky Derby Pace Projection</b><br>"
                f"<span style='font-size:13px'>Projected: {finish_order}</span><br>"
                f"<span style='font-size:11px; color:#ff4444'>■ Pacesetter</span> "
                f"<span style='font-size:11px; color:#ff8800'>■ Presser</span> "
                f"<span style='font-size:11px; color:#44cc44'>■ Stalker</span> "
                f"<span style='font-size:11px; color:#4488ff'>■ Closer</span>"
            ),
            font=dict(size=16),
        ),
        scene=dict(
            xaxis=dict(
                title="Race Progress (1¼ Miles)",
                tickvals=x_vals,
                ticktext=CALL_LABELS,
            ),
            yaxis=dict(title="Lengths Behind Leader", autorange="reversed"),
            zaxis=dict(title="Position (higher = closer to lead)"),
            camera=dict(eye=dict(x=2.0, y=-1.8, z=0.6)),
            bgcolor="#1a1a2e",
        ),
        legend=dict(
            yanchor="top", y=0.99,
            xanchor="left", x=0.01,
            bgcolor="rgba(20,20,40,0.9)",
            font=dict(color="white", size=9),
        ),
        paper_bgcolor="#0f0f23",
        margin=dict(l=0, r=0, b=0, t=100),
    )

    fig.write_html(output, include_plotlyjs="cdn")
    print(f"\n{'='*60}")
    print(f"  GRANDPA JOE — 2026 KENTUCKY DERBY 3D PROJECTION")
    print(f"{'='*60}")
    print(f"\n  Projected Finish Order:")
    for i, p in enumerate(projections[:10], 1):
        print(f"    {i:2d}. {p['name']:20s}  Beyer:{p['beyer']:3d}  [{p['style']:10s}]  {p['trainer']}")
    print(f"\n  Full field: {len(projections)} horses")
    print(f"  Saved to: {output}")
    print(f"  Open in browser to interact (rotate, zoom, hover)")
    print(f"{'='*60}\n")


def main():
    projections = project_derby(ENTRIES)
    render_3d(projections, output=str(REPO / "derby_2026_3d.html"))


if __name__ == "__main__":
    main()
