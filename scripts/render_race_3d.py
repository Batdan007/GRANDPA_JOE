"""Render a 3D race projection using plotly.

Generates an interactive HTML file showing each horse's projected path
through the race at each call point, based on their pace history averages.

Usage:
  python scripts/render_race_3d.py <race_id>
  python scripts/render_race_3d.py <race_id> --output race.html
  python scripts/render_race_3d.py --demo   # pick a populated race automatically
"""

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

try:
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

from grandpa_joe.brain.racing_brain import RacingBrain
from grandpa_joe.models.pace import race_to_3d_data, CALL_ORDER, CALL_LABELS

STYLE_COLORS = {
    "front-runner": "#ff4444",
    "presser": "#ff8800",
    "stalker": "#44aa44",
    "closer": "#4488ff",
    "plodder": "#888888",
    "unknown": "#cccccc",
}


def find_demo_race(brain) -> int:
    """Find a real race with good pace data for demo."""
    conn = brain._connect()
    try:
        row = conn.execute("""
            SELECT ra.id, ra.race_date, t.code, COUNT(DISTINCT h.horse_id) as n_horses
            FROM races ra
            JOIN tracks t ON ra.track_id = t.id
            JOIN entries e ON e.race_id = ra.id
            JOIN horse_pace_history h ON h.horse_id = e.horse_id
              AND h.race_date < ra.race_date
            WHERE ra.purse IS NOT NULL
            GROUP BY ra.id
            HAVING n_horses >= 6
            ORDER BY n_horses DESC, ra.race_date DESC
            LIMIT 1
        """).fetchone()
        if row:
            print(f"Demo race: {row['code']} on {row['race_date']} "
                  f"(race_id={row['id']}, {row['n_horses']} horses with pace data)")
            return row["id"]
    finally:
        conn.close()
    return None


def render_3d(data: dict, output_path: str = "race_3d.html"):
    """Create an interactive 3D plotly visualization."""
    if not PLOTLY_AVAILABLE:
        print("ERROR: pip install plotly")
        return

    fig = go.Figure()
    call_indices = list(range(len(CALL_ORDER)))
    call_labels = data["call_labels"]

    for i, horse in enumerate(data["horses"]):
        positions = []
        times = []
        lbs = []
        labels = []

        for j, call in enumerate(horse["calls"]):
            pos = call["position"] if call["position"] else data["field_size"]
            positions.append(pos)
            t = call["time"] if call["time"] else 0
            times.append(t)
            lb = call["lb"] if call["lb"] else 0
            lbs.append(lb)
            labels.append(
                f"{horse['name']}<br>"
                f"{call['label']}: pos {call['position']}, "
                f"{call['lb']:.1f}L behind<br>"
                f"Time: {call['time']:.1f}s" if call['time'] else
                f"{horse['name']}<br>{call['label']}: no data"
            )

        color = STYLE_COLORS.get(horse["style"], "#cccccc")
        sf_text = f" (SF avg {horse['speed_figure_avg']})" if horse['speed_figure_avg'] else ""

        # 3D line: X = call point, Y = lengths behind leader, Z = position in field
        fig.add_trace(go.Scatter3d(
            x=call_indices,
            y=lbs,
            z=[data["field_size"] + 1 - p for p in positions],  # invert so higher = better
            mode="lines+markers",
            name=f"{horse['name']} [{horse['style']}]{sf_text}",
            line=dict(color=color, width=5),
            marker=dict(size=6, color=color),
            text=labels,
            hoverinfo="text",
        ))

        # Add horse name label at finish
        fig.add_trace(go.Scatter3d(
            x=[call_indices[-1] + 0.3],
            y=[lbs[-1]],
            z=[data["field_size"] + 1 - positions[-1]],
            mode="text",
            text=[horse["name"]],
            textfont=dict(size=10, color=color),
            showlegend=False,
            hoverinfo="skip",
        ))

    # Layout
    projected = data.get("projected_finish_order", [])
    title_suffix = f"<br>Projected finish: {' > '.join(projected[:5])}" if projected else ""

    fig.update_layout(
        title=dict(
            text=f"Grandpa Joe Race Projection (3D){title_suffix}",
            font=dict(size=16),
        ),
        scene=dict(
            xaxis=dict(
                title="Race Progress",
                tickvals=call_indices,
                ticktext=call_labels,
            ),
            yaxis=dict(title="Lengths Behind Leader", autorange="reversed"),
            zaxis=dict(title="Position (higher = closer to lead)"),
            camera=dict(
                eye=dict(x=1.8, y=-1.5, z=0.8),
            ),
        ),
        legend=dict(
            yanchor="top", y=0.99,
            xanchor="left", x=0.01,
            bgcolor="rgba(255,255,255,0.8)",
        ),
        margin=dict(l=0, r=0, b=0, t=80),
        template="plotly_dark",
    )

    fig.write_html(output_path, include_plotlyjs="cdn")
    print(f"3D race render saved to: {output_path}")
    print(f"Open in browser to interact (rotate, zoom, hover).")


def render_2d_track(data: dict, output_path: str = "race_track.html"):
    """Create a 2D animated track view as a fallback."""
    if not PLOTLY_AVAILABLE:
        print("ERROR: pip install plotly")
        return

    fig = go.Figure()
    call_labels = data["call_labels"]

    for horse in data["horses"]:
        positions = [
            c["position"] if c["position"] else data["field_size"]
            for c in horse["calls"]
        ]
        color = STYLE_COLORS.get(horse["style"], "#cccccc")
        sf = horse.get("speed_figure_avg", "?")

        fig.add_trace(go.Scatter(
            x=call_labels,
            y=positions,
            mode="lines+markers+text",
            name=f"{horse['name']} (SF:{sf})",
            line=dict(color=color, width=3),
            marker=dict(size=8),
            text=[horse["name"]] + [""] * (len(positions) - 1),
            textposition="top center",
        ))

    fig.update_layout(
        title="Grandpa Joe — Projected Running Position per Call",
        xaxis_title="Race Progress",
        yaxis_title="Position (1 = lead)",
        yaxis=dict(autorange="reversed"),
        template="plotly_dark",
        height=600,
    )

    fig.write_html(output_path, include_plotlyjs="cdn")
    print(f"2D track render saved to: {output_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("race_id", nargs="?", type=int, help="Race ID to render")
    ap.add_argument("--demo", action="store_true", help="Auto-pick a demo race")
    ap.add_argument("--output", "-o", default="race_3d.html")
    ap.add_argument("--flat", action="store_true", help="2D track view instead of 3D")
    args = ap.parse_args()

    brain = RacingBrain()

    if args.demo:
        race_id = find_demo_race(brain)
        if not race_id:
            print("No races with pace data found. Run backfill_pace.py first.")
            return
    elif args.race_id:
        race_id = args.race_id
    else:
        print("Usage: render_race_3d.py <race_id> or --demo")
        return

    data = race_to_3d_data(brain, race_id)
    if not data["horses"]:
        print(f"No entries found for race {race_id}")
        return

    print(f"Rendering {data['field_size']} horses...")
    print(f"Projected finish: {' > '.join(data['projected_finish_order'][:5])}")

    if args.flat:
        render_2d_track(data, args.output)
    else:
        render_3d(data, args.output)


if __name__ == "__main__":
    main()
