"""Single-page predicted order of finish for the entire Derby Day card.
Quick-glance card for the track. Top 4 per race, two-column letter-size layout.
"""
import json
from pathlib import Path
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas

REPO = Path(__file__).resolve().parent.parent
PICKS = REPO / "data" / "full_card_picks.json"
OUT = REPO / "data" / "derby_day_2026_quick_finish.pdf"


def main():
    with open(PICKS) as f:
        data = json.load(f)
    races = data["races"]

    page_w, page_h = letter
    margin = 0.3 * inch
    c = canvas.Canvas(str(OUT), pagesize=letter)

    # Title bar
    c.setFillColor(colors.HexColor("#1a1a2e"))
    c.rect(0, page_h - 0.55 * inch, page_w, 0.55 * inch, fill=1, stroke=0)
    c.setFillColor(colors.HexColor("#ffd700"))
    c.setFont("Helvetica-Bold", 16)
    c.drawCentredString(page_w / 2, page_h - 0.32 * inch,
                        "DERBY DAY 2026 - PREDICTED ORDER OF FINISH")
    c.setFillColor(colors.white)
    c.setFont("Helvetica", 9)
    c.drawCentredString(page_w / 2, page_h - 0.49 * inch,
                        f"Churchill Downs - Saturday May 2, 2026 - "
                        f"Generated {datetime.now().strftime('%a %b %d %I:%M %p')} - "
                        f"Grandpa Joe / DJR")

    # Two-column layout
    col_w = (page_w - 3 * margin) / 2
    col_left_x = margin
    col_right_x = margin * 2 + col_w
    top_y = page_h - 0.75 * inch
    footer_top_y = 0.95 * inch  # reserve bottom for legend + rebuild commands
    block_h = (top_y - footer_top_y) / 7  # 7 races per column

    races_sorted = [r for r in races if r["horses"]]
    races_sorted.sort(key=lambda r: r["race"])

    for idx, r in enumerate(races_sorted):
        col = 0 if idx < 7 else 1
        row = idx if idx < 7 else idx - 7
        x = col_left_x if col == 0 else col_right_x
        y_top = top_y - row * block_h
        y_bottom = y_top - block_h

        # Confidence-colored race header bar
        conf = r.get("single_confidence", "WIDE")
        bar_color = {
            "STRONG": colors.HexColor("#28a745"),
            "MEDIUM": colors.HexColor("#ffc107"),
            "WIDE":   colors.HexColor("#6c757d"),
        }.get(conf, colors.HexColor("#6c757d"))
        c.setFillColor(bar_color)
        c.rect(x, y_top - 0.22 * inch, col_w, 0.22 * inch, fill=1, stroke=0)

        # Race number + name + post time + confidence
        c.setFillColor(colors.white if conf != "MEDIUM" else colors.black)
        c.setFont("Helvetica-Bold", 10)
        race_label = f"R{r['race']}"
        if r["race"] == 12:
            race_label += " - KENTUCKY DERBY"
        c.drawString(x + 0.06 * inch, y_top - 0.155 * inch, race_label)

        # Right side of header: time + confidence tag
        post_time = r.get("post_time", "?")
        pace = r.get("pace_shape", "?")
        right_text = f"{post_time}  |  pace: {pace}  |  {conf}"
        c.setFont("Helvetica", 8)
        c.drawRightString(x + col_w - 0.06 * inch, y_top - 0.155 * inch, right_text)

        # Top 4 horses
        horses = r["horses"][:4]
        c.setFillColor(colors.black)
        for i, h in enumerate(horses):
            row_y = y_top - 0.36 * inch - i * 0.16 * inch
            # Position number (1-4)
            c.setFont("Helvetica-Bold", 11)
            color = (colors.HexColor("#28a745") if i == 0
                     else colors.HexColor("#444"))
            c.setFillColor(color)
            c.drawString(x + 0.08 * inch, row_y, f"{i+1}.")
            # PP
            c.setFont("Helvetica-Bold", 9)
            c.setFillColor(colors.HexColor("#1a1a2e"))
            c.drawString(x + 0.28 * inch, row_y,
                         f"PP{h['post']}")
            # Name
            c.setFillColor(colors.black)
            c.setFont("Helvetica" if i > 0 else "Helvetica-Bold", 9)
            star = " *" if h.get("power_pick") else ""
            name_display = (h["name"][:24] + "..."
                            if len(h["name"]) > 24 else h["name"])
            c.drawString(x + 0.7 * inch, row_y, f"{name_display}{star}")
            # ML odds (right-aligned)
            c.setFont("Helvetica", 8)
            c.setFillColor(colors.HexColor("#666"))
            ml = h.get("ml") or "-"
            c.drawRightString(x + col_w - 0.1 * inch, row_y, ml)

        # Faint bottom border
        c.setStrokeColor(colors.HexColor("#cccccc"))
        c.setLineWidth(0.5)
        c.line(x, y_bottom + 0.04 * inch,
               x + col_w, y_bottom + 0.04 * inch)

    # ── Footer: divider line ───────────────────────────────────────────
    c.setStrokeColor(colors.HexColor("#1a1a2e"))
    c.setLineWidth(1.2)
    c.line(margin, 0.92 * inch, page_w - margin, 0.92 * inch)

    # ── Row 1: Visual color legend ─────────────────────────────────────
    legend_y = 0.66 * inch
    legend_h = 0.18 * inch
    swatch_w = 0.22 * inch
    gap = 0.05 * inch

    # "LEGEND:" label
    c.setFillColor(colors.HexColor("#1a1a2e"))
    c.setFont("Helvetica-Bold", 9)
    cur_x = margin + 0.05 * inch
    c.drawString(cur_x, legend_y + 0.05 * inch, "LEGEND:")
    cur_x += 0.55 * inch

    # Three confidence swatches with labels
    swatches = [
        ("#28a745", "SINGLE", "SINGLE this leg",   colors.white),
        ("#ffc107", "MED",    "use 2 horses",      colors.black),
        ("#6c757d", "WIDE",   "use 3-4 horses",    colors.white),
    ]
    for hex_color, swatch_label, full_label, text_color in swatches:
        c.setFillColor(colors.HexColor(hex_color))
        c.rect(cur_x, legend_y, swatch_w, legend_h, fill=1, stroke=0)
        c.setFillColor(text_color)
        c.setFont("Helvetica-Bold", 7.5)
        c.drawCentredString(cur_x + swatch_w / 2,
                             legend_y + 0.06 * inch, swatch_label)
        cur_x += swatch_w + gap
        c.setFillColor(colors.HexColor("#333"))
        c.setFont("Helvetica", 8.5)
        c.drawString(cur_x, legend_y + 0.05 * inch, f"= {full_label}")
        cur_x += c.stringWidth(f"= {full_label}", "Helvetica", 8.5) + 0.22 * inch

    # Star marker explanation
    c.setFillColor(colors.HexColor("#1a1a2e"))
    c.setFont("Helvetica-Bold", 9)
    c.drawString(cur_x, legend_y + 0.05 * inch, "*")
    c.setFillColor(colors.HexColor("#333"))
    c.setFont("Helvetica", 8.5)
    c.drawString(cur_x + 0.08 * inch, legend_y + 0.05 * inch,
                 "= DRF Power Pick")

    # ── Row 2: Singling-candidate callout ──────────────────────────────
    c.setFillColor(colors.HexColor("#28a745"))
    c.setFont("Helvetica-Bold", 8.5)
    c.drawString(margin + 0.05 * inch, 0.48 * inch, "SINGLES TODAY:")
    c.setFillColor(colors.HexColor("#333"))
    c.setFont("Helvetica", 8.5)
    c.drawString(margin + 1.0 * inch, 0.48 * inch,
                 "R10 Crazy Mason   -   R12 Renegade   -   R13 Buetane   "
                 "(single these legs in Pick 4/5/6 to slash combos)")

    # ── Row 3: Rebuild commands (PowerShell, monospace box) ────────────
    cmd_y = 0.18 * inch
    cmd_h = 0.22 * inch
    c.setFillColor(colors.HexColor("#0f0f23"))
    c.rect(margin, cmd_y, page_w - 2 * margin, cmd_h, fill=1, stroke=0)
    c.setFillColor(colors.HexColor("#ffd700"))
    c.setFont("Helvetica-Bold", 7.5)
    c.drawString(margin + 0.06 * inch, cmd_y + 0.13 * inch, "REBUILD (PS):")
    c.setFillColor(colors.HexColor("#a0e0ff"))
    c.setFont("Courier-Bold", 7.5)
    cmd_text = (
        "git pull origin main; "
        "python scripts\\process_full_card.py; "
        "python scripts\\animate_full_card.py; "
        "python scripts\\build_derby_betting_pdf.py; "
        "python scripts\\build_quick_finish_card.py"
    )
    c.drawString(margin + 1.0 * inch, cmd_y + 0.13 * inch, cmd_text)
    c.setFillColor(colors.HexColor("#888"))
    c.setFont("Helvetica", 6.5)
    c.drawString(margin + 0.06 * inch, cmd_y + 0.03 * inch,
                 "From C:\\Users\\danie\\GRANDPA_JOE  -  refreshes picks, "
                 "animations, betting sheet, and this card")

    c.save()
    print(f"[OK] {OUT}")
    print(f"     Single-page letter-size PDF, 14 races, top 4 per race")


if __name__ == "__main__":
    main()
