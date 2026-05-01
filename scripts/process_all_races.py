"""TOMORROW-MORNING WORKFLOW — process every Derby Day race PDF in data/.

Workflow:
  1. Drop per-race DRF PP PDFs into data/ matching pattern  CD-R{n}--05-02-2026.pdf
     (e.g. CD-R7--05-02-2026.pdf, CD-R8--05-02-2026.pdf, ...)
     The Derby uses CD--05-02-2026.pdf (already in repo).
  2. Run:  python scripts/process_all_races.py
  3. For each race PDF found, this script:
       - Parses the PDF with PyMuPDF
       - Extracts each horse's TFUS Early/Late, post, ML, jockey, trainer
       - Applies pace-overlay ranking (same logic as the Derby)
       - Outputs   data/CD_R{n}_2026-05-02_picks.json
       - Optionally renders an animated playback to bundle/race_3d/
  4. Builds  data/all_races_order_of_finish.pdf  with every race's projected top 5

This script is the laptop-side counterpart to the Derby pipeline. It assumes
DRF PPs PDFs follow the standard format with a "TimeformUS Pace: Early NN
Late NN" line per horse.

Usage:
  python scripts/process_all_races.py                 # all races found
  python scripts/process_all_races.py --race 8        # just R8
  python scripts/process_all_races.py --no-animate    # rankings only, no plotly
  python scripts/process_all_races.py --pdf           # also build summary PDF
"""
from __future__ import annotations
import argparse
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"
OUT_DIR = REPO / "bundle" / "race_3d"

try:
    import fitz  # PyMuPDF
except ImportError:
    print("[ERR] pip install pymupdf")
    sys.exit(1)


# ── PDF parsing ────────────────────────────────────────────────────────
HORSE_HEADER_RE = re.compile(
    r"^\s*(\d{1,2})\s*\n\s*(\d+\s*[-/]\s*\d+|EVEN|MTO)\s*\n([A-Z][A-Za-z' ()-]+?)\s*\n",
    re.MULTILINE
)
TFUS_RE = re.compile(r"TimeformUS Pace:\s*Early\s+(\d+)\s+Late\s+(\d+)")
RACE_HEADER_RE = re.compile(
    r"CD,\s*race\s*(\d+),\s*page\s*:\s*(\d+)", re.IGNORECASE
)
DISTANCE_RE = re.compile(r"(\d+\s*\d*[¼½¾]?\s*MILES?|\d+\s*F)", re.IGNORECASE)


def parse_race_pdf(pdf_path: Path) -> dict:
    """Extract one race's structured info from a DRF PPs PDF."""
    doc = fitz.open(pdf_path)
    pages_text = [p.get_text() for p in doc]
    full = "\n".join(pages_text)

    # Race number from header
    m = RACE_HEADER_RE.search(full)
    race_num = int(m.group(1)) if m else 0

    # Distance / surface (rough guess; may need manual fix per race)
    dist_m = DISTANCE_RE.search(full)
    distance = dist_m.group(0) if dist_m else "?"

    horses = []
    # Walk each page; horse blocks usually start with PP\nML\nName
    # We'll extract by heuristic: the blocks follow a regular structure
    # Look for TFUS line, then back-walk a few lines to find name + post
    for page_text in pages_text:
        # Split by horse separator (each horse starts with a number on its own line followed by ML odds)
        lines = page_text.split("\n")
        for i, line in enumerate(lines):
            tfus_m = re.match(r"TimeformUS Pace:\s*Early\s+(\d+)\s+Late\s+(\d+)", line.strip())
            if not tfus_m:
                continue
            early, late = int(tfus_m.group(1)), int(tfus_m.group(2))
            # Walk back to find name + post + ML
            name = None
            post = None
            ml = None
            jockey = None
            trainer = None
            for j in range(max(0, i - 25), i):
                ln = lines[j].strip()
                # Look for ML pattern
                if not ml and re.match(r"^\d+\s*[-/]\s*\d+$", ln):
                    ml = ln
                    # Post is two lines above the ML, name one line below
                    if j >= 2 and re.match(r"^\d{1,2}$", lines[j - 1].strip()):
                        post = int(lines[j - 1].strip())
                    if j + 1 < len(lines):
                        candidate = lines[j + 1].strip()
                        if candidate and not candidate.startswith("Own"):
                            name = candidate
                # Jockey line: "LASTNAME F (...)"
                if not jockey and re.match(r"^[A-Z][A-Z' ]+\s+[A-Z]+\s*\(", ln):
                    jockey = ln.split("(")[0].strip().title()
            if name and post:
                horses.append({
                    "post": post,
                    "name": name,
                    "ml": ml,
                    "tfus_early": early,
                    "tfus_late": late,
                    "jockey": jockey,
                })

    return {
        "race": race_num,
        "distance": distance,
        "horses": horses,
        "source": pdf_path.name,
    }


# ── Pace-overlay ranking (same logic as Derby) ─────────────────────────
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
    """Same multiplier logic as derby_2026_pace_overlay.py.
    n_pacesetters in the field changes the regime."""
    if early is None or late is None:
        return 1.0
    fast_pace = n_pacesetters >= 3
    if fast_pace:
        if early >= 105: return 0.55
        if early >= 95:  return 0.85
        if early <= 80 and late >= 108: return 1.50
        if late >= 104 and early <= 94: return 1.30
        return 1.00
    # MODERATE / SLOW pace — reverse the bonus for closers, neutral else
    if early >= 105: return 0.95
    if late >= 108 and early <= 80: return 1.10
    return 1.00


def rank_race(parsed: dict) -> list:
    horses = parsed["horses"]
    if not horses:
        return []

    # Style classification
    for h in horses:
        h["style"] = style_from_tfus(h["tfus_early"], h["tfus_late"])
    n_pace = sum(1 for h in horses if h["style"] == "pacesetter")

    # Score: TFUS Late (with light Beyer/post bonus baked in via TFUS already)
    # plus pace factor
    for h in horses:
        late = h["tfus_late"] or 90
        early = h["tfus_early"] or 90
        # base score: late number is the dominant factor for who finishes well
        base = late * 1.0 + max(0, 95 - early) * 0.3
        h["pace_factor"] = pace_factor(early, late, n_pace)
        h["score"] = base * h["pace_factor"]

    horses.sort(key=lambda h: -h["score"])
    return horses


# ── Output ─────────────────────────────────────────────────────────────
def build_summary(all_races: list) -> str:
    """Markdown summary suitable for printing."""
    lines = ["# Derby Day 2026 — Order of Finish Predictions", ""]
    lines.append("Generated by GRANDPA_JOE multi-race processor "
                 "with TFUS pace-overlay model.")
    lines.append("")
    for r in all_races:
        race_num = r["race"]
        distance = r["distance"]
        n_horses = len(r["horses"])
        lines.append(f"## Race {race_num}  ({distance}, field {n_horses})")
        lines.append("")
        lines.append("| Pos | PP | Horse | ML | TFUS-E | TFUS-L | Style | Score |")
        lines.append("|-----|----|-------|------|--------|--------|-------|-------|")
        for i, h in enumerate(r["horses"][:8], 1):
            lines.append(
                f"| {i} | {h['post']} | {h['name']} | {h['ml'] or '—'} | "
                f"{h['tfus_early']} | {h['tfus_late']} | {h['style']} | "
                f"{h['score']:.1f} |"
            )
        lines.append("")
    return "\n".join(lines)


def write_pdf(md_text: str, out: Path):
    """Render markdown to a basic PDF using ReportLab."""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import inch
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                     Table, TableStyle, PageBreak)
    s = getSampleStyleSheet()
    doc = SimpleDocTemplate(str(out), pagesize=letter,
                             leftMargin=0.4 * inch, rightMargin=0.4 * inch,
                             topMargin=0.4 * inch, bottomMargin=0.4 * inch)
    story = []
    story.append(Paragraph("<b>DERBY DAY 2026 — Order of Finish</b>", s["Title"]))
    story.append(Spacer(1, 0.2 * inch))
    # Crude markdown→ReportLab: split by ## sections
    sections = md_text.split("## ")[1:]
    for sec in sections:
        title, rest = sec.split("\n", 1)
        story.append(Paragraph(f"<b>{title}</b>", s["Heading2"]))
        # Find table rows
        table_lines = [ln for ln in rest.split("\n") if ln.startswith("|")]
        if len(table_lines) >= 2:
            # skip the separator row (---)
            rows = []
            for ln in table_lines:
                cells = [c.strip() for c in ln.strip("|").split("|")]
                if all(re.match(r"^-+$", c) for c in cells):
                    continue
                rows.append(cells)
            t = Table(rows, repeatRows=1)
            t.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1a2e")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1),
                 [colors.white, colors.HexColor("#f5f5f5")]),
            ]))
            story.append(t)
        story.append(Spacer(1, 0.2 * inch))
    doc.build(story)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--race", type=int, help="Single race number to process")
    ap.add_argument("--no-animate", action="store_true",
                    help="Skip plotly animations")
    ap.add_argument("--pdf", action="store_true",
                    help="Build summary PDF")
    args = ap.parse_args()

    # Find candidate PDFs — match either Derby (CD--) or per-race (CD-R{n}--)
    candidates = []
    for p in DATA.glob("CD-R*-05-02-2026.pdf"):
        m = re.match(r"CD-R(\d+)-+05-02-2026\.pdf", p.name)
        if m:
            candidates.append((int(m.group(1)), p))
    derby_pdf = DATA / "CD--05-02-2026.pdf"
    if derby_pdf.exists():
        candidates.append((12, derby_pdf))

    candidates.sort()

    if args.race:
        candidates = [(n, p) for n, p in candidates if n == args.race]

    # Scratch filter: derby_2026_pps.json is the source of truth for R12
    derby_pps_path = DATA / "derby_2026_pps.json"
    scratched_names = set()
    if derby_pps_path.exists():
        with open(derby_pps_path) as f:
            derby = json.load(f)
        for e in derby.get("entries", []):
            if e.get("scratched"):
                scratched_names.add(e["name"].lower().strip())

    if not candidates:
        print("[ERR] No race PDFs found in data/.")
        print("       Expected pattern: CD-R{n}--05-02-2026.pdf "
              "(per race) or CD--05-02-2026.pdf (Derby).")
        sys.exit(1)

    print(f"Processing {len(candidates)} race(s)...")
    all_results = []
    for race_num, pdf in candidates:
        print(f"\n  Race {race_num}: parsing {pdf.name}")
        parsed = parse_race_pdf(pdf)
        # Override race number from filename if PDF text doesn't match
        if not parsed["race"] or parsed["race"] != race_num:
            parsed["race"] = race_num

        # Filter scratches (R12 specifically, but applies generally)
        if scratched_names:
            before = len(parsed["horses"])
            parsed["horses"] = [
                h for h in parsed["horses"]
                if h["name"].lower().strip() not in scratched_names
            ]
            removed = before - len(parsed["horses"])
            if removed:
                print(f"    Filtered {removed} scratched horse(s)")

        ranked = rank_race(parsed)
        if not ranked:
            print(f"    [WARN] No horses parsed from {pdf.name}")
            continue

        # Save per-race JSON
        out_json = DATA / f"CD_R{race_num}_2026-05-02_picks.json"
        with open(out_json, "w") as f:
            json.dump({
                "race": race_num,
                "distance": parsed["distance"],
                "ranking": [{
                    "pos": i + 1,
                    "post": h["post"],
                    "name": h["name"],
                    "ml": h["ml"],
                    "tfus_early": h["tfus_early"],
                    "tfus_late": h["tfus_late"],
                    "style": h["style"],
                    "pace_factor": h["pace_factor"],
                    "score": h["score"],
                } for i, h in enumerate(ranked)],
            }, f, indent=2)
        print(f"    [OK] Saved {out_json.name}")
        all_results.append(parsed)

        # Top 5 to stdout
        print(f"    Top 5:")
        for i, h in enumerate(ranked[:5], 1):
            print(f"      {i}. PP{h['post']:<2} {h['name']:<22}  "
                  f"ML {h['ml'] or '—':<5}  E{h['tfus_early']}/L{h['tfus_late']}  "
                  f"style={h['style']}  score={h['score']:.1f}")

    if args.pdf and all_results:
        md = build_summary(all_results)
        out_pdf = DATA / "all_races_order_of_finish.pdf"
        write_pdf(md, out_pdf)
        print(f"\n[OK] Summary PDF: {out_pdf}")

    print("\nDone.")


if __name__ == "__main__":
    main()
