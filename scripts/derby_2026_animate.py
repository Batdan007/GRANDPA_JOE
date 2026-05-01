"""Animated Derby playback — full 20-horse field running counter-clockwise
around a Churchill-style oval from gate to wire, with TFUS Early/Late pace
figures driving each horse's per-frame position.

Reads data/derby_2026_pps.json (single source of truth — already has scratches
filtered, AEs included, and TFUS pace figs from the DRF parse).

Output: bundle/race_3d/derby_2026_animated.html
"""
from __future__ import annotations
import argparse
import json
import math
import sys
import webbrowser
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PPS = REPO / "data" / "derby_2026_pps.json"
OUT_DIR = REPO / "bundle" / "race_3d"

try:
    import plotly.graph_objects as go
except ImportError:
    print("[ERR] pip install plotly")
    sys.exit(1)


CALL_KEYS = ["S", "Q", "H", "T", "STR", "F"]
CALL_FRAC = {"S": 0.00, "Q": 0.20, "H": 0.40, "T": 0.60, "STR": 0.85, "F": 1.00}
CALL_PRETTY = {"S": "Start", "Q": "1/4", "H": "1/2",
               "T": "3/4", "STR": "Stretch", "F": "Wire"}

STYLE_PROFILES = {
    "pacesetter":  {"S": 3,  "Q": 2,  "H": 2,  "T": 3,  "STR": 5,  "F": 8},
    "presser":     {"S": 6,  "Q": 5,  "H": 4,  "T": 4,  "STR": 4,  "F": 5},
    "stalker":     {"S": 10, "Q": 9,  "H": 8,  "T": 6,  "STR": 5,  "F": 4},
    "closer":      {"S": 16, "Q": 15, "H": 14, "T": 11, "STR": 7,  "F": 3},
    "deep_closer": {"S": 18, "Q": 17, "H": 16, "T": 13, "STR": 8,  "F": 4},
    "unknown":     {"S": 10, "Q": 9,  "H": 8,  "T": 7,  "STR": 6,  "F": 6},
}
STYLE_COLORS = {
    "pacesetter":  "#ff4444",
    "presser":     "#ff8800",
    "stalker":     "#44cc44",
    "closer":      "#4488ff",
    "deep_closer": "#9966cc",
    "unknown":     "#aaaaaa",
}
STYLE_LANE = {
    "pacesetter":  0.05,
    "presser":     0.20,
    "stalker":     0.40,
    "closer":      0.60,
    "deep_closer": 0.75,
    "unknown":     0.35,
}


def style_from_tfus_or_label(early, late, label):
    """Use TFUS Early/Late if available; else fall back to JSON label.
    Promote 'closer' with very low Early to 'deep_closer'."""
    if early is not None and late is not None:
        gap = early - late
        if gap >= 8:
            return "pacesetter"
        if gap >= 3:
            return "presser"
        if gap >= -3:
            return "stalker"
        if gap >= -10:
            return "closer"
        return "deep_closer"
    return label or "unknown"


# ── Oval track geometry (Churchill ~1 mile main track) ────────────────
TRACK_A = 6.0
TRACK_R = 2.0
WIRE_X, WIRE_Y = TRACK_A, -TRACK_R


def oval_position(u: float, lane_offset: float = 0.0):
    a = TRACK_A + TRACK_R + lane_offset
    b = TRACK_R + lane_offset
    start_ang = math.pi * 1.10
    end_ang = math.pi * 0.0
    ang = start_ang + (end_ang - start_ang - 2 * math.pi) * u
    return a * math.cos(ang), b * math.sin(ang)


def oval_outline(lane_offset=0):
    xs, ys = [], []
    for k in range(201):
        ang = (k / 200) * 2 * math.pi
        a = TRACK_A + TRACK_R + lane_offset
        b = TRACK_R + lane_offset
        xs.append(a * math.cos(ang))
        ys.append(b * math.sin(ang))
    return xs, ys


def beyer_adj(beyer, bmax, bmin):
    if not beyer or bmax == bmin:
        return 0
    return -3 * ((beyer - bmin) / (bmax - bmin) - 0.5)


def trajectory(profile, badj, n_frames):
    call_w = {"S": 0.1, "Q": 0.2, "H": 0.4, "T": 0.6, "STR": 0.8, "F": 1.0}
    ranks = {c: profile[c] + badj * call_w[c] for c in CALL_KEYS}
    out = []
    for k in range(n_frames):
        t = k / (n_frames - 1)
        for j in range(len(CALL_KEYS) - 1):
            c1, c2 = CALL_KEYS[j], CALL_KEYS[j + 1]
            f1, f2 = CALL_FRAC[c1], CALL_FRAC[c2]
            if f1 <= t <= f2 or (j == len(CALL_KEYS) - 2 and t >= f2):
                w = (t - f1) / max(1e-6, f2 - f1)
                rank = ranks[c1] * (1 - w) + ranks[c2] * w
                lb = max(0.0, (rank - 1) * 1.2)
                out.append((t, rank, lb))
                break
    return out


RACE_LENGTHS_FOR_VIZ = 80.0  # smaller = more visual separation


def build_derby_animation(n_frames=140):
    with open(PPS) as f:
        data = json.load(f)

    # Active starters only
    raw = [e for e in data["entries"]
           if e.get("drew_in") and not e.get("scratched")]

    entries = []
    for e in raw:
        st = style_from_tfus_or_label(
            e.get("tfus_early"), e.get("tfus_late"), e.get("style"))
        entries.append({
            "name": e["name"],
            "post": e["post_position"],
            "ml": e.get("morning_line", "—"),
            "trainer": e.get("trainer", "—"),
            "jockey": e.get("jockey", "—"),
            "beyer": e.get("beyer") or 90,
            "style": st,
            "tfus_e": e.get("tfus_early"),
            "tfus_l": e.get("tfus_late"),
            "auxiliary": bool(e.get("auxiliary_gate")),
        })

    beyers = [x["beyer"] for x in entries if x["beyer"]]
    bmax = max(beyers) if beyers else 100
    bmin = min(beyers) if beyers else 90
    field_size = len(entries)

    for e in entries:
        profile = STYLE_PROFILES.get(e["style"], STYLE_PROFILES["stalker"])
        scaled = {c: profile[c] * (field_size / 20.0) for c in CALL_KEYS}
        badj = beyer_adj(e["beyer"], bmax, bmin)
        e["traj"] = trajectory(scaled, badj, n_frames)
        # Auxiliary gate horses start a bit wider
        e["lane"] = STYLE_LANE.get(e["style"], 0.35) + (0.25 if e["auxiliary"] else 0.0)

    def horse_xy(e, k):
        t_lead = k / (n_frames - 1)
        _, rank, lb = e["traj"][k]
        long_offset = lb / RACE_LENGTHS_FOR_VIZ
        u = max(0.0, t_lead - long_offset)
        return oval_position(u, lane_offset=e["lane"]), rank, lb

    # Pre-compute final order of finish for the wire-zone panel
    finish_order = sorted(entries, key=lambda x: x["traj"][-1][1])
    finish_lines = []
    for pos, e in enumerate(finish_order[:8], 1):
        ml = f" {e['ml']}" if e['ml'] else ""
        finish_lines.append(f"{pos}. PP{e['post']:>2} {e['name']}{ml}")
    finish_panel_text = "ORDER OF FINISH<br>" + "<br>".join(finish_lines)
    wire_zone_start = n_frames - max(10, n_frames // 7)

    # Build animation frames
    frames = []
    for k in range(n_frames):
        xs, ys, colors_, texts = [], [], [], []
        # Compute current rank order for sub-display
        snapshot = []
        for e in entries:
            (x, y), rank, lb = horse_xy(e, k)
            snapshot.append((rank, e, lb))
            xs.append(x)
            ys.append(y)
            colors_.append(STYLE_COLORS.get(e["style"], "#ccc"))
        # Sort to get current running order
        snapshot.sort(key=lambda r: r[0])
        order_strs = []
        for pos, (rank, e, lb) in enumerate(snapshot[:6], 1):
            order_strs.append(f"{pos}. PP{e['post']} {e['name']}")

        for e in entries:
            (x, y), rank, lb = horse_xy(e, k)
            tfus_str = (f"TFUS E{e['tfus_e']}/L{e['tfus_l']}"
                        if e['tfus_e'] is not None else "TFUS —")
            texts.append(
                f"<b>PP{e['post']} {e['name']}</b><br>"
                f"Rank: {rank:.1f}/{field_size}  •  Lb: {lb:.1f}<br>"
                f"Style: {e['style']}<br>"
                f"{tfus_str}  •  Beyer {e['beyer']}<br>"
                f"ML: {e['ml']}  •  {e['jockey']}"
            )
        # Build per-frame annotation: running order while racing,
        # ORDER OF FINISH panel as we approach the wire
        if k >= wire_zone_start:
            order_panel = dict(
                text=finish_panel_text,
                xref="paper", yref="paper",
                x=0.99, y=0.98, xanchor="right", yanchor="top",
                showarrow=False,
                font=dict(family="Courier New", size=12, color="#ffd700"),
                bgcolor="rgba(0,0,0,0.92)", bordercolor="#ffd700",
                borderwidth=2, borderpad=8,
                align="left",
            )
        else:
            order_panel = dict(
                text="RUNNING ORDER<br>" + "<br>".join(order_strs),
                xref="paper", yref="paper",
                x=0.01, y=0.98, xanchor="left", yanchor="top",
                showarrow=False,
                font=dict(family="Courier New", size=11, color="#ffd700"),
                bgcolor="rgba(0,0,0,0.6)",
                bordercolor="#444", borderwidth=1, borderpad=6,
                align="left",
            )

        # Include call-point labels in every frame (else they'd disappear during play)
        call_anns = []
        for c, frac in CALL_FRAC.items():
            cx, cy = oval_position(frac, lane_offset=1.4)
            call_anns.append(dict(
                x=cx, y=cy, text=CALL_PRETTY[c], showarrow=False,
                font=dict(color="#ffd700", size=11),
                bgcolor="rgba(0,0,0,0.55)", borderpad=2,
            ))

        frames.append(go.Frame(
            data=[go.Scatter(
                x=xs, y=ys, mode="markers+text",
                marker=dict(size=22, color=colors_,
                            line=dict(color="#000", width=2)),
                text=[f"{e['post']}" for e in entries],
                textposition="middle center",
                textfont=dict(color="white", size=11, family="Arial Black"),
                hovertext=texts, hoverinfo="text",
                name="horses",
            )],
            name=str(k),
            layout=dict(annotations=call_anns + [order_panel]),
        ))

    # Initial frame state
    init_xs, init_ys, init_colors, init_texts = [], [], [], []
    for e in entries:
        (x, y), rank, lb = horse_xy(e, 0)
        init_xs.append(x)
        init_ys.append(y)
        init_colors.append(STYLE_COLORS.get(e["style"], "#ccc"))
        init_texts.append(
            f"<b>PP{e['post']} {e['name']}</b><br>"
            f"Style: {e['style']}<br>"
            f"ML: {e['ml']}"
        )

    rail_x, rail_y = oval_outline(0)
    out_x, out_y = oval_outline(1.0)

    fig = go.Figure(
        data=[
            go.Scatter(x=rail_x, y=rail_y, mode="lines",
                       line=dict(color="#555", width=2),
                       showlegend=False, hoverinfo="skip"),
            go.Scatter(x=out_x, y=out_y, mode="lines",
                       line=dict(color="#555", width=2, dash="dot"),
                       showlegend=False, hoverinfo="skip"),
            go.Scatter(x=[WIRE_X + 0.1], y=[WIRE_Y - 0.4],
                       mode="text", text=["FINISH"],
                       textfont=dict(size=14, color="#ffd700"),
                       showlegend=False, hoverinfo="skip"),
            go.Scatter(
                x=init_xs, y=init_ys, mode="markers+text",
                marker=dict(size=22, color=init_colors,
                            line=dict(color="#000", width=2)),
                text=[f"{e['post']}" for e in entries],
                textposition="middle center",
                textfont=dict(color="white", size=11, family="Arial Black"),
                hovertext=init_texts, hoverinfo="text",
                name="horses",
            ),
        ],
        frames=frames,
    )

    finish_top = " > ".join(
        f"PP{e['post']} {e['name']}"
        for e in sorted(entries, key=lambda x: x["traj"][-1][1])[:4]
    )

    fig.update_layout(
        title=dict(
            text=(
                "<b>GRANDPA JOE — 152nd KENTUCKY DERBY (May 2 2026)</b><br>"
                "<span style='font-size:13px'>Pace-adjusted projection w/ TFUS Early/Late "
                "&nbsp;•&nbsp; Field 20  &nbsp;•&nbsp; 1¼ Miles Dirt</span><br>"
                f"<span style='font-size:11px;color:#ffd700'>Projected: {finish_top}</span><br>"
                "<span style='font-size:11px'>"
                "<span style='color:#ff4444'>● Pacesetter</span>  "
                "<span style='color:#ff8800'>● Presser</span>  "
                "<span style='color:#44cc44'>● Stalker</span>  "
                "<span style='color:#4488ff'>● Closer</span>  "
                "<span style='color:#9966cc'>● Deep Closer</span>"
                "</span>"
            ),
            x=0.5, xanchor="center",
        ),
        xaxis=dict(visible=False, range=[-(TRACK_A + TRACK_R + 2),
                                          TRACK_A + TRACK_R + 2],
                   scaleanchor="y", scaleratio=1),
        yaxis=dict(visible=False, range=[-(TRACK_R + 2), TRACK_R + 2]),
        plot_bgcolor="#0a4d0a",
        paper_bgcolor="#0a0a0a",
        font=dict(color="#f0f0f0"),
        height=760,
        margin=dict(l=20, r=20, t=120, b=80),
        updatemenus=[dict(
            type="buttons",
            showactive=False,
            x=0.5, xanchor="center", y=-0.04, yanchor="top",
            buttons=[
                dict(label="▶ Play",
                     method="animate",
                     args=[None, dict(
                         frame=dict(duration=70, redraw=True),
                         fromcurrent=True,
                         transition=dict(duration=30, easing="linear"),
                     )]),
                dict(label="⏸ Pause",
                     method="animate",
                     args=[[None], dict(
                         frame=dict(duration=0, redraw=False),
                         mode="immediate",
                     )]),
                dict(label="⏮ Reset",
                     method="animate",
                     args=[["0"], dict(
                         frame=dict(duration=0, redraw=True),
                         mode="immediate",
                     )]),
            ],
        )],
        sliders=[dict(
            active=0,
            x=0.05, len=0.9, y=-0.01,
            steps=[dict(
                method="animate",
                args=[[str(k)], dict(
                    frame=dict(duration=0, redraw=True),
                    mode="immediate",
                )],
                label=f"{k * 100 // (n_frames - 1)}%"
            ) for k in range(0, n_frames, max(1, n_frames // 20))],
            currentvalue=dict(prefix="Race progress: ", visible=True,
                              font=dict(color="#ffd700", size=12)),
        )],
    )

    # Initial-state annotations (call points + running order panel for frame 0)
    init_anns = []
    for c, frac in CALL_FRAC.items():
        x, y = oval_position(frac, lane_offset=1.4)
        init_anns.append(dict(
            x=x, y=y, text=CALL_PRETTY[c], showarrow=False,
            font=dict(color="#ffd700", size=11),
            bgcolor="rgba(0,0,0,0.55)", borderpad=2,
        ))
    init_anns.append(dict(
        text="RUNNING ORDER<br>(press Play to start)",
        xref="paper", yref="paper",
        x=0.01, y=0.98, xanchor="left", yanchor="top",
        showarrow=False,
        font=dict(family="Courier New", size=11, color="#ffd700"),
        bgcolor="rgba(0,0,0,0.6)",
        bordercolor="#444", borderwidth=1, borderpad=6,
    ))
    fig.update_layout(annotations=init_anns)

    return fig, entries


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--frames", type=int, default=140,
                    help="number of animation frames (default 140 ~10s @ 70ms)")
    ap.add_argument("--open", action="store_true",
                    help="open in default browser after rendering")
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig, entries = build_derby_animation(n_frames=args.frames)
    out = OUT_DIR / "derby_2026_animated.html"
    fig.write_html(str(out), include_plotlyjs="cdn",
                   auto_play=False)

    print("=" * 70)
    print(f"  GRANDPA JOE — 2026 DERBY ANIMATED RACE PLAYBACK")
    print("=" * 70)
    print(f"  Field:  {len(entries)} horses (scratches filtered)")
    print(f"  Frames: {args.frames}  (~{args.frames * 0.07:.0f}s real time)")
    print(f"  Output: {out}")
    print()
    print("  Style mix:")
    style_count = {}
    for e in entries:
        style_count[e["style"]] = style_count.get(e["style"], 0) + 1
    for s, n in sorted(style_count.items(), key=lambda kv: -kv[1]):
        print(f"    {s:<13s} {n}")
    print()
    print("  Open in browser, hit Play to watch the race.")
    print("=" * 70)

    if args.open:
        webbrowser.open(out.as_uri())


if __name__ == "__main__":
    main()
