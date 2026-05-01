"""Generate animated race playback for every race on the full card.

Reads data/full_card_picks.json (output of process_full_card.py),
renders one Plotly animated HTML per race into bundle/race_3d/, plus
an updated index.html with all 14 races.
"""
from __future__ import annotations
import argparse
import json
import math
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"
OUT_DIR = REPO / "bundle" / "race_3d"
PICKS = DATA / "full_card_picks.json"

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
}
STYLE_COLORS = {
    "pacesetter":  "#ff4444",
    "presser":     "#ff8800",
    "stalker":     "#44cc44",
    "closer":      "#4488ff",
    "deep_closer": "#9966cc",
}
STYLE_LANE = {
    "pacesetter":  0.05,
    "presser":     0.20,
    "stalker":     0.40,
    "closer":      0.60,
    "deep_closer": 0.75,
}

TRACK_A = 6.0
TRACK_R = 2.0
WIRE_X, WIRE_Y = TRACK_A, -TRACK_R
RACE_LENGTHS_FOR_VIZ = 80.0


def style_from_tfus(early, late, fallback="stalker"):
    if early is None or late is None:
        return fallback
    gap = early - late
    if gap >= 8: return "pacesetter"
    if gap >= 3: return "presser"
    if gap >= -3: return "stalker"
    if gap >= -10: return "closer"
    return "deep_closer"


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


def trajectory(profile, n_frames=120):
    out = []
    for k in range(n_frames):
        t = k / (n_frames - 1)
        for j in range(len(CALL_KEYS) - 1):
            c1, c2 = CALL_KEYS[j], CALL_KEYS[j + 1]
            f1, f2 = CALL_FRAC[c1], CALL_FRAC[c2]
            if f1 <= t <= f2 or (j == len(CALL_KEYS) - 2 and t >= f2):
                w = (t - f1) / max(1e-6, f2 - f1)
                rank = profile[c1] * (1 - w) + profile[c2] * w
                lb = max(0.0, (rank - 1) * 1.2)
                out.append((t, rank, lb))
                break
    return out


def render_race_animation(race: dict, n_frames=120) -> str:
    horses = race["horses"]
    if not horses:
        return None

    field_size = len(horses)
    entries = []
    for h in horses:
        st = h.get("style") or style_from_tfus(h.get("tfus_early"), h.get("tfus_late"))
        if st not in STYLE_PROFILES:
            st = "stalker"
        profile = STYLE_PROFILES[st]
        scaled = {c: profile[c] * (field_size / 20.0) for c in CALL_KEYS}
        entries.append({
            "name": h["name"],
            "post": h["post"],
            "ml": h.get("ml", "—"),
            "tfus_e": h.get("tfus_early"),
            "tfus_l": h.get("tfus_late"),
            "score": h.get("score", 0),
            "rank": h.get("rank"),
            "style": st,
            "lane": STYLE_LANE.get(st, 0.35),
            "traj": trajectory(scaled, n_frames),
            "power_pick": h.get("power_pick", False),
        })

    def horse_xy(e, k):
        t_lead = k / (n_frames - 1)
        _, rank, lb = e["traj"][k]
        long_offset = lb / RACE_LENGTHS_FOR_VIZ
        u = max(0.0, t_lead - long_offset)
        return oval_position(u, lane_offset=e["lane"]), rank, lb

    # Pre-compute final order of finish (using last-frame rank)
    finish_order = sorted(entries, key=lambda x: x["traj"][-1][1])
    finish_lines = []
    for pos, e in enumerate(finish_order[:8], 1):
        star = " ★" if e["power_pick"] else ""
        ml = f" {e['ml']}" if e['ml'] else ""
        finish_lines.append(f"{pos}. PP{e['post']:>2} {e['name']}{star}{ml}")
    finish_panel_text = "ORDER OF FINISH<br>" + "<br>".join(finish_lines)

    # Static call-point annotations (Start, 1/4, Stretch, Wire, etc.)
    call_annotations = []
    for c, frac in CALL_FRAC.items():
        x, y = oval_position(frac, lane_offset=1.4)
        call_annotations.append(dict(
            x=x, y=y, text=CALL_PRETTY[c], showarrow=False,
            font=dict(color="#ffd700", size=10),
            bgcolor="rgba(0,0,0,0.55)", borderpad=2,
        ))

    def frame_annotations(k):
        """Build full annotation set for frame k: call points + running/finish order."""
        snapshot = []
        for e in entries:
            _, rank, _ = e["traj"][k]
            snapshot.append((rank, e))
        snapshot.sort(key=lambda r: r[0])

        # Show ORDER OF FINISH on last 15% of frames (the wire shot)
        wire_zone = k >= n_frames - max(8, n_frames // 7)

        if wire_zone:
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
            lines = []
            for pos, (_, e) in enumerate(snapshot[:6], 1):
                lines.append(f"{pos}. PP{e['post']} {e['name']}")
            order_panel = dict(
                text="RUNNING ORDER<br>" + "<br>".join(lines),
                xref="paper", yref="paper",
                x=0.01, y=0.98, xanchor="left", yanchor="top",
                showarrow=False,
                font=dict(family="Courier New", size=10, color="#ffd700"),
                bgcolor="rgba(0,0,0,0.6)",
                bordercolor="#444", borderwidth=1, borderpad=5,
                align="left",
            )

        return call_annotations + [order_panel]

    frames = []
    for k in range(n_frames):
        xs, ys, colors_, texts = [], [], [], []
        for e in entries:
            (x, y), rank, lb = horse_xy(e, k)
            xs.append(x); ys.append(y)
            colors_.append(STYLE_COLORS.get(e["style"], "#ccc"))
            star = " ★" if e["power_pick"] else ""
            tfus = (f"E{e['tfus_e']}/L{e['tfus_l']}" if e['tfus_e'] is not None
                    else "TFUS —")
            texts.append(
                f"<b>PP{e['post']} {e['name']}{star}</b><br>"
                f"Rank: {rank:.1f}/{field_size}  •  Lb: {lb:.1f}<br>"
                f"{e['style']}  •  {tfus}<br>"
                f"ML: {e['ml']}  •  Score: {e['score']:.1f}"
            )
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
            layout=dict(annotations=frame_annotations(k)),
        ))

    init_xs, init_ys, init_colors, init_texts = [], [], [], []
    for e in entries:
        (x, y), rank, lb = horse_xy(e, 0)
        init_xs.append(x); init_ys.append(y)
        init_colors.append(STYLE_COLORS.get(e["style"], "#ccc"))
        init_texts.append(f"<b>PP{e['post']} {e['name']}</b><br>{e['style']}")

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

    title = (
        f"<b>R{race['race']} — {race.get('post_time','?')} ET</b>"
        f"<span style='font-size:12px'> · {race.get('distance','?')} · "
        f"{race.get('surface','?')} · {race.get('purse','?')} · "
        f"pace={race.get('pace_shape','?')} · "
        f"confidence={race.get('single_confidence','?')}</span><br>"
        f"<span style='font-size:11px;color:#ffd700'>Projected: {finish_top}</span>"
    )

    fig.update_layout(
        title=dict(text=title, x=0.5, xanchor="center"),
        xaxis=dict(visible=False, range=[-(TRACK_A + TRACK_R + 2),
                                          TRACK_A + TRACK_R + 2],
                   scaleanchor="y", scaleratio=1),
        yaxis=dict(visible=False, range=[-(TRACK_R + 2), TRACK_R + 2]),
        plot_bgcolor="#0a4d0a", paper_bgcolor="#0a0a0a",
        font=dict(color="#f0f0f0"),
        height=620, margin=dict(l=20, r=20, t=80, b=70),
        annotations=frame_annotations(0),  # initial state matches frame 0
        updatemenus=[dict(
            type="buttons", showactive=False,
            x=0.5, xanchor="center", y=-0.04, yanchor="top",
            buttons=[
                dict(label="Play", method="animate",
                     args=[None, dict(frame=dict(duration=70, redraw=True),
                                      fromcurrent=True,
                                      transition=dict(duration=30))]),
                dict(label="Pause", method="animate",
                     args=[[None], dict(frame=dict(duration=0, redraw=False),
                                        mode="immediate")]),
            ],
        )],
        sliders=[dict(active=0, x=0.05, len=0.9, y=-0.01,
            steps=[dict(method="animate",
                args=[[str(k)], dict(frame=dict(duration=0, redraw=True),
                                      mode="immediate")],
                label=f"{k * 100 // (n_frames - 1)}%")
                for k in range(0, n_frames, max(1, n_frames // 20))],
            currentvalue=dict(prefix="Race progress: ", visible=True,
                              font=dict(color="#ffd700", size=12)),
        )],
    )
    # Call points already included in frame_annotations(); no separate add needed

    out = OUT_DIR / f"R{race['race']}_full_card_animated.html"
    fig.write_html(str(out), include_plotlyjs="cdn", auto_play=False)
    return out


INDEX_TEMPLATE = """<!DOCTYPE html>
<html><head><title>CD Derby Day 2026 — Full Card</title>
<style>
body{{font-family:system-ui;background:#1a1a1a;color:#f0f0f0;padding:30px;
  max-width:980px;margin:auto}}
h1{{color:#ffd700;border-bottom:2px solid #ffd700;padding-bottom:8px}}
h2{{color:#ffd700;margin-top:30px}}
table{{border-collapse:collapse;width:100%;margin:14px 0}}
th,td{{padding:8px 10px;border-bottom:1px solid #333;text-align:left}}
th{{background:#222;color:#ffd700}}
tr:hover{{background:#222}}
a{{color:#66bbff;text-decoration:none}}
a:hover{{text-decoration:underline}}
.tag{{display:inline-block;font-size:10px;padding:2px 6px;border-radius:3px;
  margin-left:6px;font-weight:bold}}
.t-strong{{background:#28a745;color:#fff}}
.t-medium{{background:#ffc107;color:#000}}
.t-wide{{background:#dc3545;color:#fff}}
.t-derby{{background:#ffd700;color:#000}}
.race-num{{font-weight:bold;color:#ffd700}}
.power{{color:#ff8800}}
</style></head><body>
<h1>GRANDPA JOE — Churchill Downs Derby Day 2026 (Full Card)</h1>
<p>Saturday May 2 2026 — TFUS pace-overlay rankings + DRF Power Picks · 14 races</p>
<p>Confidence: <span class="tag t-strong">STRONG</span> = single this leg ·
   <span class="tag t-medium">MEDIUM</span> = 2 horses ·
   <span class="tag t-wide">WIDE</span> = 3-4 horses</p>

<table>
<tr><th>Race</th><th>Time</th><th>Distance</th><th>Pace</th><th>Top Pick (gap)</th>
    <th>Confidence</th><th>Animation</th></tr>
{rows}
</table>

<p style="margin-top:30px;color:#aaa;font-size:13px">
Generated by <code>scripts/process_full_card.py</code> +
<code>scripts/animate_full_card.py</code>.<br>
Re-run anytime after dropping fresh PPs into <code>data/Friday pps/</code>.
</p>
</body></html>"""


def build_index(races: list, anim_paths: dict):
    rows = []
    for r in races:
        if not r["horses"]:
            continue
        conf = r.get("single_confidence", "?")
        tag_class = "t-strong" if conf == "STRONG" else (
            "t-medium" if conf == "MEDIUM" else "t-wide")
        is_derby = r["race"] == 12
        rn_html = f'<span class="race-num">R{r["race"]}</span>'
        if is_derby:
            rn_html += '<span class="tag t-derby">DERBY</span>'
        gap = r.get("gap_1_2", 0)
        top = r["horses"][0]["name"]
        if r["horses"][0].get("power_pick"):
            top = f'<span class="power">{top} ★</span>'
        anim = anim_paths.get(r["race"])
        anim_link = (f'<a href="{anim.name}">Watch race ▶</a>'
                     if anim else "—")
        rows.append(
            f"<tr><td>{rn_html}</td>"
            f"<td>{r.get('post_time','?')}</td>"
            f"<td>{r.get('distance','?')} {r.get('surface','')}</td>"
            f"<td>{r.get('pace_shape','?')}</td>"
            f"<td>{top} ({gap:.1f})</td>"
            f"<td><span class='tag {tag_class}'>{conf}</span></td>"
            f"<td>{anim_link}</td></tr>"
        )
    out = OUT_DIR / "full_card_index.html"
    out.write_text(INDEX_TEMPLATE.format(rows="\n".join(rows)),
                   encoding="utf-8")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--frames", type=int, default=120)
    ap.add_argument("--race", type=int, help="Single race only")
    args = ap.parse_args()

    if not PICKS.exists():
        print(f"[ERR] {PICKS} not found. Run scripts/process_full_card.py first.")
        sys.exit(1)

    with open(PICKS) as f:
        data = json.load(f)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    races = data["races"]
    if args.race:
        races = [r for r in races if r["race"] == args.race]

    anim_paths = {}
    print(f"Rendering {len(races)} race animations to {OUT_DIR}")
    for r in races:
        if not r["horses"]:
            continue
        out = render_race_animation(r, n_frames=args.frames)
        if out:
            anim_paths[r["race"]] = out
            print(f"  R{r['race']:<2}  ->  {out.name}  "
                  f"({len(r['horses'])} horses)")

    if not args.race:
        index = build_index(data["races"], anim_paths)
        print(f"\n[OK] Full card index: {index}")


if __name__ == "__main__":
    main()
