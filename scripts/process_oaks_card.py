"""Oaks Day (May 1, 2026) processor — parses Friday's DRF PPs PDF for the
entire Oaks Day card, applies the pace-overlay model, builds Pick 4/5
strategy, and produces:

  - data/oaks_day_picks.json
  - data/oaks_day_2026_quick_finish.pdf  (1-page order of finish + bet sheet)
  - bundle/race_3d/oaks_R{n}_animated.html  (per-race animations)

The May 1 PDF is the regular DRF format (different from Sat's TFUS Pace
Projector). TFUS Early/Late lines look like:
    TimeformUS Pace: Early 95
    Late 63
"""
from __future__ import annotations
import argparse
import json
import re
import sys
from pathlib import Path
from datetime import datetime

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"
OUT_DIR = REPO / "bundle" / "race_3d"

OAKS_PDF = DATA / "Friday pps" / "CD--05-01-2026.pdf"

try:
    import fitz
except ImportError:
    print("[ERR] pip install pymupdf")
    sys.exit(1)


# ── DRF PPs parser (multi-line TFUS) ───────────────────────────────────
RACE_HEADER_RE = re.compile(
    r"\bCD,\s*race\s*(\d+),\s*page\s*:\s*(\d+)", re.IGNORECASE
)
TFUS_MULTILINE_RE = re.compile(
    r"TimeformUS Pace:\s*Early\s+(\d+)\s*\n\s*Late\s+(\d+)", re.IGNORECASE
)
TFUS_INLINE_RE = re.compile(
    r"TimeformUS Pace:\s*Early\s+(\d+)\s+Late\s+(\d+)", re.IGNORECASE
)


def split_by_race(full_text: str):
    """Yield (race_num, race_text) chunks. The DRF PPs PDF has continuous
    page numbering across races, so we split when the race number changes."""
    # Find every "CD, race N, page:M" marker
    markers = []
    for m in re.finditer(r"\bCD,\s*race\s*(\d+),\s*page\s*:\s*\d+",
                          full_text, re.IGNORECASE):
        markers.append((m.start(), int(m.group(1))))

    if not markers:
        return

    # Find boundaries: positions where race number changes
    boundaries = [(markers[0][0], markers[0][1])]
    for pos, race_num in markers[1:]:
        if race_num != boundaries[-1][1]:
            boundaries.append((pos, race_num))

    for i, (pos, race_num) in enumerate(boundaries):
        end = boundaries[i + 1][0] if i + 1 < len(boundaries) else len(full_text)
        yield race_num, full_text[pos:end]


def parse_race(race_num: int, text: str) -> dict:
    """Parse one race's chunk for horses + TFUS pace."""
    # Find horse blocks. Each horse starts with: PP\nML-odds\nName
    # Then has "TimeformUS Pace: Early N \n Late N" embedded
    horses = []

    # Use a simpler approach: find every TFUS pair and walk back to find horse meta
    lines = text.split("\n")
    for i, line in enumerate(lines):
        m = re.match(r"\s*TimeformUS Pace:\s*Early\s+(\d+)\s*$", line.strip())
        if m:
            tfus_e = int(m.group(1))
            # Late should be the next line (or two)
            tfus_l = None
            for j in range(i + 1, min(i + 3, len(lines))):
                lm = re.match(r"\s*(?:Late\s+)?(\d+)\s*$", lines[j].strip())
                if lm and "Late" in lines[j]:
                    tfus_l = int(lm.group(1))
                    break
                lm2 = re.match(r"^Late\s+(\d+)", lines[j].strip())
                if lm2:
                    tfus_l = int(lm2.group(1))
                    break

            # Walk backward for ML, post, name
            ml = None
            post = None
            name = None
            for j in range(max(0, i - 12), i):
                ln = lines[j].strip()
                if not ml and re.match(r"^\d+\s*[-/]\s*\d+$", ln):
                    ml = ln
                    if j >= 2 and re.match(r"^\d{1,2}$", lines[j - 1].strip()):
                        post = int(lines[j - 1].strip())
                    if j + 1 < len(lines):
                        candidate = lines[j + 1].strip()
                        if (candidate and not candidate.startswith("Own")
                                and not candidate.startswith("M:")
                                and not candidate.startswith("LB")):
                            name = candidate
            if name and post is not None and tfus_l is not None:
                horses.append({
                    "post": post,
                    "name": name,
                    "ml": ml,
                    "tfus_early": tfus_e,
                    "tfus_late": tfus_l,
                })

    # Also try inline regex as fallback
    for m in TFUS_INLINE_RE.finditer(text):
        # If we already have a horse with this Early/Late pair, skip
        e, l = int(m.group(1)), int(m.group(2))
        if any(h["tfus_early"] == e and h["tfus_late"] == l for h in horses):
            continue
        # Walk back to find name + post + ML
        before = text[:m.start()].split("\n")
        for back in range(min(15, len(before))):
            ln = before[-(back + 1)].strip()
            if re.match(r"^\d+\s*[-/]\s*\d+$", ln):
                ml = ln
                # post is line above
                if back + 2 <= len(before):
                    post_line = before[-(back + 2)].strip()
                    if re.match(r"^\d{1,2}$", post_line):
                        try:
                            post = int(post_line)
                        except ValueError:
                            continue
                        # name is line below ML
                        name_line = before[-back] if back > 0 else None
                        if not name_line and back == 0:
                            name_line = ""
                        try:
                            name = (text[m.start():].split("\n")[0]
                                    if False else None)  # not reliable
                        except Exception:
                            name = None
                break

    # Get race meta (post time, distance)
    head = text[:1500]
    post_time = None
    distance = None
    purse = None
    surface = "dirt"
    pt = re.search(r"Post time:\s*(\d{1,2}:\d{2}\s*[AP]?M?\s*ET?)", head)
    if pt: post_time = pt.group(1).strip()
    pu = re.search(r"Purse\s*\$([\d,]+)", head)
    if pu: purse = "$" + pu.group(1)
    di = re.search(r"(\d+(?:\s*\d+/\d+)?\s*(?:MILES?|FURLONGS?))", head, re.IGNORECASE)
    if di: distance = di.group(0).strip()
    if "Turf)" in head or "(Turf)" in head:
        surface = "turf"

    # Race name
    name_m = re.search(r"\b([A-Z][A-Za-z][A-Za-z' \-]+(?:STAKES?|CLASSIC|DERBY|OAKS|HANDICAP|MILE|SPRINT))",
                        head)
    race_name = name_m.group(0).strip() if name_m else None

    return {
        "race": race_num,
        "name": race_name,
        "post_time": post_time,
        "distance": distance,
        "surface": surface,
        "purse": purse,
        "horses": horses,
    }


def parse_oaks_pdf(pdf_path: Path) -> list:
    doc = fitz.open(pdf_path)
    full = "\n".join(p.get_text() for p in doc)
    races = []
    for race_num, text in split_by_race(full):
        race = parse_race(race_num, text)
        if race["horses"]:
            races.append(race)
    return races


# ── Pace overlay scoring ───────────────────────────────────────────────
def style_from_tfus(early, late):
    if early is None or late is None:
        return "stalker"
    gap = early - late
    if gap >= 8: return "pacesetter"
    if gap >= 3: return "presser"
    if gap >= -3: return "stalker"
    if gap >= -10: return "closer"
    return "deep_closer"


def pace_factor(early, late, n_pacesetters):
    if early is None or late is None:
        return 1.0
    fast = n_pacesetters >= 3
    if fast:
        if early >= 105: return 0.55
        if early >= 95:  return 0.85
        if early <= 80 and late >= 108: return 1.50
        if late >= 104 and early <= 94: return 1.30
        return 1.00
    if early >= 105: return 0.95
    if late >= 108 and early <= 80: return 1.10
    return 1.00


def rank_race(race: dict) -> dict:
    horses = race["horses"]
    for h in horses:
        h["style"] = style_from_tfus(h["tfus_early"], h["tfus_late"])
    n_pace = sum(1 for h in horses if h["style"] == "pacesetter"
                 or (h["tfus_early"] and h["tfus_early"] >= 100))

    for h in horses:
        late = h["tfus_late"] or 90
        early = h["tfus_early"] or 90
        base = late * 1.0 + max(0, 95 - early) * 0.3
        h["pace_factor"] = pace_factor(early, late, n_pace)
        h["score"] = base * h["pace_factor"]

    horses.sort(key=lambda h: -h["score"])

    if len(horses) >= 2:
        race["gap_1_2"] = horses[0]["score"] - horses[1]["score"]
        if race["gap_1_2"] >= 15:
            race["single_confidence"] = "STRONG"
        elif race["gap_1_2"] >= 8:
            race["single_confidence"] = "MEDIUM"
        else:
            race["single_confidence"] = "WIDE"
    else:
        race["single_confidence"] = "UNKNOWN"

    race["n_pacesetters"] = n_pace
    race["pace_shape"] = "FAST" if n_pace >= 3 else ("MOD" if n_pace == 2 else "SLOW/TAC")
    return race


# ── PDF output (1-page Oaks Day card) ───────────────────────────────────
def write_oaks_pdf(races: list, out: Path):
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.pdfgen import canvas

    page_w, page_h = letter
    margin = 0.3 * inch
    c = canvas.Canvas(str(out), pagesize=letter)

    # Title
    c.setFillColor(colors.HexColor("#9d2235"))  # Oaks pink/maroon
    c.rect(0, page_h - 0.55 * inch, page_w, 0.55 * inch, fill=1, stroke=0)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 16)
    c.drawCentredString(page_w / 2, page_h - 0.3 * inch,
                        "OAKS DAY 2026 - PREDICTED ORDER OF FINISH")
    c.setFont("Helvetica", 9)
    c.drawCentredString(page_w / 2, page_h - 0.46 * inch,
                        f"Churchill Downs - Friday May 1, 2026 - "
                        f"Generated {datetime.now().strftime('%a %b %d %I:%M %p')}")

    # Sort races
    races_sorted = sorted([r for r in races if r["horses"]],
                          key=lambda r: r["race"])

    col_w = (page_w - 3 * margin) / 2
    cl_x = margin
    cr_x = margin * 2 + col_w
    top_y = page_h - 0.75 * inch
    n_rows = max(1, (len(races_sorted) + 1) // 2)
    block_h = (top_y - 1.0 * inch) / n_rows

    for idx, r in enumerate(races_sorted):
        col = 0 if idx < n_rows else 1
        row = idx if idx < n_rows else idx - n_rows
        x = cl_x if col == 0 else cr_x
        y_top = top_y - row * block_h

        conf = r.get("single_confidence", "WIDE")
        bar_color = {
            "STRONG": colors.HexColor("#28a745"),
            "MEDIUM": colors.HexColor("#ffc107"),
            "WIDE":   colors.HexColor("#6c757d"),
        }.get(conf, colors.HexColor("#6c757d"))

        c.setFillColor(bar_color)
        c.rect(x, y_top - 0.22 * inch, col_w, 0.22 * inch, fill=1, stroke=0)
        c.setFillColor(colors.white if conf != "MEDIUM" else colors.black)
        c.setFont("Helvetica-Bold", 10)
        race_label = f"R{r['race']}"
        if r.get("name") and "OAKS" in r["name"].upper():
            race_label += " - KENTUCKY OAKS"
        c.drawString(x + 0.06 * inch, y_top - 0.155 * inch, race_label)

        c.setFont("Helvetica", 8)
        right_text = (f"{r.get('post_time', '?')}  |  "
                      f"pace: {r.get('pace_shape', '?')}  |  {conf}")
        c.drawRightString(x + col_w - 0.06 * inch, y_top - 0.155 * inch, right_text)

        # Top 4 horses
        c.setFillColor(colors.black)
        for i, h in enumerate(r["horses"][:4]):
            row_y = y_top - 0.36 * inch - i * 0.16 * inch
            c.setFont("Helvetica-Bold", 11)
            color = (colors.HexColor("#28a745") if i == 0
                     else colors.HexColor("#444"))
            c.setFillColor(color)
            c.drawString(x + 0.08 * inch, row_y, f"{i+1}.")
            c.setFont("Helvetica-Bold", 9)
            c.setFillColor(colors.HexColor("#1a1a2e"))
            c.drawString(x + 0.28 * inch, row_y, f"PP{h['post']}")
            c.setFillColor(colors.black)
            c.setFont("Helvetica-Bold" if i == 0 else "Helvetica", 9)
            name_disp = (h["name"][:24] + "..."
                         if len(h["name"]) > 24 else h["name"])
            c.drawString(x + 0.7 * inch, row_y, name_disp)
            c.setFont("Helvetica", 8)
            c.setFillColor(colors.HexColor("#666"))
            c.drawRightString(x + col_w - 0.1 * inch, row_y, h.get("ml") or "—")

    # Footer: legend + key bets
    c.setStrokeColor(colors.HexColor("#1a1a2e"))
    c.setLineWidth(1.2)
    c.line(margin, 0.92 * inch, page_w - margin, 0.92 * inch)

    # Legend
    swatches = [("#28a745", "SINGLE", colors.white),
                ("#ffc107", "MED", colors.black),
                ("#6c757d", "WIDE", colors.white)]
    cur_x = margin + 0.05 * inch
    c.setFillColor(colors.HexColor("#1a1a2e"))
    c.setFont("Helvetica-Bold", 9)
    c.drawString(cur_x, 0.74 * inch, "LEGEND:")
    cur_x += 0.55 * inch
    for hex_c, lbl, txt in swatches:
        c.setFillColor(colors.HexColor(hex_c))
        c.rect(cur_x, 0.66 * inch, 0.22 * inch, 0.18 * inch, fill=1, stroke=0)
        c.setFillColor(txt)
        c.setFont("Helvetica-Bold", 7.5)
        c.drawCentredString(cur_x + 0.11 * inch, 0.72 * inch, lbl)
        cur_x += 0.22 * inch + 0.05 * inch
        c.setFillColor(colors.HexColor("#333"))
        c.setFont("Helvetica", 8.5)
        meaning = {"SINGLE": "= SINGLE this leg",
                   "MED": "= use 2 horses",
                   "WIDE": "= use 3-4 horses"}[lbl]
        c.drawString(cur_x, 0.71 * inch, meaning)
        cur_x += c.stringWidth(meaning, "Helvetica", 8.5) + 0.22 * inch

    # Singling candidates
    singles = [r for r in races_sorted if r.get("single_confidence") == "STRONG"]
    if singles:
        c.setFillColor(colors.HexColor("#28a745"))
        c.setFont("Helvetica-Bold", 8.5)
        c.drawString(margin + 0.05 * inch, 0.50 * inch,
                     "SINGLES TODAY:")
        c.setFillColor(colors.HexColor("#333"))
        c.setFont("Helvetica", 8.5)
        single_text = "  -  ".join(
            f"R{r['race']} #{r['horses'][0]['post']} {r['horses'][0]['name']} (gap {r['gap_1_2']:.1f})"
            for r in singles
        )
        c.drawString(margin + 1.0 * inch, 0.50 * inch, single_text[:140])

    # Rebuild commands
    cmd_y = 0.18 * inch
    cmd_h = 0.22 * inch
    c.setFillColor(colors.HexColor("#0f0f23"))
    c.rect(margin, cmd_y, page_w - 2 * margin, cmd_h, fill=1, stroke=0)
    c.setFillColor(colors.HexColor("#ffd700"))
    c.setFont("Helvetica-Bold", 7.5)
    c.drawString(margin + 0.06 * inch, cmd_y + 0.13 * inch, "REBUILD (PS):")
    c.setFillColor(colors.HexColor("#a0e0ff"))
    c.setFont("Courier-Bold", 7.5)
    cmd_text = "git pull origin main; python scripts\\process_oaks_card.py"
    c.drawString(margin + 1.0 * inch, cmd_y + 0.13 * inch, cmd_text)
    c.setFillColor(colors.HexColor("#888"))
    c.setFont("Helvetica", 6.5)
    c.drawString(margin + 0.06 * inch, cmd_y + 0.03 * inch,
                 "From C:\\Users\\danie\\GRANDPA_JOE  -  refreshes Oaks order of finish")

    c.save()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf-path", default=str(OAKS_PDF))
    args = ap.parse_args()

    pdf = Path(args.pdf_path)
    if not pdf.exists():
        print(f"[ERR] {pdf} not found")
        sys.exit(1)

    print(f"Parsing {pdf.name}...")
    races = parse_oaks_pdf(pdf)
    print(f"  Found {len(races)} races with TFUS data")

    for r in races:
        rank_race(r)

    out_json = DATA / "oaks_day_picks.json"
    with open(out_json, "w") as f:
        json.dump({"races": races}, f, indent=2, default=str)
    print(f"  [OK] {out_json.name}")

    print()
    print("=" * 90)
    print("  CHURCHILL DOWNS OAKS DAY (May 1) - ORDER OF FINISH")
    print("=" * 90)
    print(f"  {'Race':<5} {'Horses':<7} {'Pace':<8} {'Conf':<8} {'#1 (gap)':<40} {'#2'}")
    print(f"  {'-'*5} {'-'*7} {'-'*8} {'-'*8} {'-'*40} {'-'*22}")
    for r in sorted(races, key=lambda x: x["race"]):
        if not r["horses"]:
            continue
        h = r["horses"]
        gap = r.get("gap_1_2", 0)
        top1 = (f"PP{h[0]['post']} {h[0]['name']} ML={h[0]['ml']} "
                f"({gap:.1f})")[:40]
        top2 = (f"PP{h[1]['post']} {h[1]['name']}")[:22] if len(h) > 1 else "—"
        print(f"  R{r['race']:<4} {len(h):<7} {r.get('pace_shape','?'):<8} "
              f"{r.get('single_confidence','?'):<8} {top1:<40} {top2}")

    print()
    print("  SINGLING CANDIDATES (high model confidence):")
    for r in sorted(races, key=lambda x: x["race"]):
        if r.get("single_confidence") == "STRONG":
            h = r["horses"][0]
            print(f"    R{r['race']:<2} SINGLE: PP{h['post']} {h['name']} "
                  f"({h['ml']}) - gap {r['gap_1_2']:.1f}")

    out_pdf = DATA / "oaks_day_2026_quick_finish.pdf"
    write_oaks_pdf(races, out_pdf)
    print(f"\n  [OK] {out_pdf.name}  (1-page printable)")


if __name__ == "__main__":
    main()
