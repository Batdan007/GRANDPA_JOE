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
    block_h = (top_y - margin) / 7  # 7 races per column

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

    # Footer with legend
    c.setFillColor(colors.HexColor("#666"))
    c.setFont("Helvetica", 7.5)
    legend = ("CONFIDENCE: green=SINGLE this leg | yellow=use 2 horses | "
              "gray=3-4 horses    *=DRF Power Pick    "
              "Singles: R10 Crazy Mason, R12 Renegade, R13 Buetane")
    c.drawCentredString(page_w / 2, 0.18 * inch, legend)

    c.save()
    print(f"[OK] {OUT}")
    print(f"     Single-page letter-size PDF, 14 races, top 4 per race")


if __name__ == "__main__":
    main()
