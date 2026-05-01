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

    # ── PAGE 2: $300 BEST-CHANCE BET SHEET ─────────────────────────────
    c.showPage()

    # Title bar
    c.setFillColor(colors.HexColor("#1a1a2e"))
    c.rect(0, page_h - 0.55 * inch, page_w, 0.55 * inch, fill=1, stroke=0)
    c.setFillColor(colors.HexColor("#ffd700"))
    c.setFont("Helvetica-Bold", 16)
    c.drawCentredString(page_w / 2, page_h - 0.32 * inch,
                        "$300 BANKROLL - BEST-CHANCE BET SHEET")
    c.setFillColor(colors.white)
    c.setFont("Helvetica", 9)
    c.drawCentredString(page_w / 2, page_h - 0.49 * inch,
                        "1 EXA + 1 TRI + 1 SUP for the Derby + Pick 4/5/6 - "
                        "structured for high cash probability")

    # Bet definitions ($300 bankroll, sized for cash probability + value)
    bets = [
        {
            "n": "1",
            "name": "DERBY EXACTA",
            "race": "R12 Kentucky Derby (6:57 ET)",
            "structure": "BOX 1, 6  ($5 base x 2 combos)",
            "horses": "PP1 Renegade  /  PP6 Commandment",
            "rationale": "Model's two top picks; either order cashes. "
                          "Renegade L116 + Commandment L105 are the two best late kicks.",
            "cost": 10.00,
        },
        {
            "n": "2",
            "name": "DERBY TRIFECTA",
            "race": "R12 Kentucky Derby",
            "structure": "KEY 6  /  1, 9  /  1, 9, 5  ($2 base x 6 combos)",
            "horses": "Commandment KEY over Renegade, Puma  with Right to Party 3rd",
            "rationale": "Commandment is the pace-adjusted #1 (+5.7pp edge); "
                          "Renegade/Puma are the model's #2-3 with Right to Party as the bomb cover.",
            "cost": 12.00,
        },
        {
            "n": "3",
            "name": "DERBY SUPERFECTA",
            "race": "R12 Kentucky Derby",
            "structure": "6  /  1, 9, 5  /  1, 9, 5, 8  /  1, 9, 5, 8, 18  ($1 base x 60 combos)",
            "horses": "Cascading wheel under Commandment (covers all top-5 + Further Ado 4th)",
            "rationale": "Tight enough to cash with the model's top picks, "
                          "wide enough to catch Right to Party at 30-1 in 4th for the boom.",
            "cost": 60.00,
        },
        {
            "n": "4",
            "name": "PICK 4  (R9-R12)",
            "race": "R9 - R10 - R11 - R12  (4:06-6:57 ET)",
            "structure": "2 / SINGLE / 3 / 4 horses  ($2 base x 24 combos)",
            "horses": "R9: Let's Be Frank, Remember Mamba  |  R10: CRAZY MASON SINGLE  |  "
                      "R11: Mercante, Program Trading, Test Score  |  R12: 6, 1, 9, 5",
            "rationale": "Singling Crazy Mason in R10 (gap 20.8) cuts combo count by 65% "
                          "vs. a typical 3-3-3-4 spread.",
            "cost": 48.00,
        },
        {
            "n": "5",
            "name": "PICK 5  (R8-R12)",
            "race": "R8 - R12  (3:23-6:57 ET)",
            "structure": "3 / 2 / SINGLE / 3 / 4 horses  ($1 base x 72 combos)",
            "horses": "R8: Englishman, Crude Velocity, Crown the Buckeye  |  "
                      "R9-R12 same as Pick 4",
            "rationale": "Adds the Pat Day Mile (R8) leg. Englishman is the chalk; "
                          "Crude Velocity + Buckeye are the live overlays.",
            "cost": 72.00,
        },
        {
            "n": "6",
            "name": "PICK 6  (R7-R12)",
            "race": "R7 - R12  (2:38-6:57 ET)",
            "structure": "3 / 3 / 2 / SINGLE / 3 / 3 horses  ($0.50 base x 162 combos)",
            "horses": "R7: Pin Up Betty, Temptable GB, Vina Arana  |  R8-R12 same as Pick 5  |  "
                      "R12 trimmed to 6, 1, 9 (drop Right to Party from this leg)",
            "rationale": "Highest payout potential; Crazy Mason single in R10 is the value "
                          "engine. Trimming Derby to 3 keeps cost manageable at 50c base.",
            "cost": 81.00,
        },
    ]

    # Render each bet as a card
    top_y = page_h - 0.75 * inch
    bet_h = 1.08 * inch  # ~6 bets at 1.08" = 6.5", leaves room for total
    bet_x = margin
    bet_w = page_w - 2 * margin

    for i, bet in enumerate(bets):
        y_top = top_y - i * bet_h
        y_bot = y_top - bet_h

        # Row stripe (alternating)
        if i % 2 == 0:
            c.setFillColor(colors.HexColor("#f8f9fa"))
        else:
            c.setFillColor(colors.HexColor("#ffffff"))
        c.rect(bet_x, y_bot + 0.03 * inch, bet_w,
               bet_h - 0.06 * inch, fill=1, stroke=0)

        # Number circle
        c.setFillColor(colors.HexColor("#1a1a2e"))
        c.circle(bet_x + 0.32 * inch, y_top - 0.22 * inch, 0.18 * inch, fill=1, stroke=0)
        c.setFillColor(colors.HexColor("#ffd700"))
        c.setFont("Helvetica-Bold", 14)
        c.drawCentredString(bet_x + 0.32 * inch, y_top - 0.27 * inch, bet["n"])

        # Bet name + race
        c.setFillColor(colors.HexColor("#1a1a2e"))
        c.setFont("Helvetica-Bold", 12)
        c.drawString(bet_x + 0.6 * inch, y_top - 0.22 * inch, bet["name"])
        c.setFillColor(colors.HexColor("#666"))
        c.setFont("Helvetica", 8)
        c.drawString(bet_x + 0.6 * inch, y_top - 0.36 * inch, bet["race"])

        # Cost (right-aligned, big)
        c.setFillColor(colors.HexColor("#28a745"))
        c.setFont("Helvetica-Bold", 16)
        c.drawRightString(bet_x + bet_w - 0.15 * inch,
                          y_top - 0.27 * inch, f"${bet['cost']:.2f}")

        # Structure line
        c.setFillColor(colors.HexColor("#0f0f23"))
        c.setFont("Courier-Bold", 8.5)
        c.drawString(bet_x + 0.18 * inch, y_top - 0.55 * inch,
                     "STRUCTURE: " + bet["structure"])

        # Horses
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 8)
        c.drawString(bet_x + 0.18 * inch, y_top - 0.71 * inch,
                     "HORSES: " + bet["horses"][:130])
        if len(bet["horses"]) > 130:
            c.drawString(bet_x + 0.78 * inch, y_top - 0.83 * inch,
                         bet["horses"][130:260])

        # Rationale
        c.setFillColor(colors.HexColor("#444"))
        c.setFont("Helvetica-Oblique", 7.5)
        c.drawString(bet_x + 0.18 * inch, y_top - 0.97 * inch,
                     "WHY: " + bet["rationale"][:155])

        # Punched checkbox
        c.setStrokeColor(colors.HexColor("#1a1a2e"))
        c.setLineWidth(1.2)
        c.rect(bet_x + bet_w - 0.45 * inch, y_top - 0.55 * inch,
               0.18 * inch, 0.18 * inch, fill=0, stroke=1)
        c.setFillColor(colors.HexColor("#666"))
        c.setFont("Helvetica", 6.5)
        c.drawCentredString(bet_x + bet_w - 0.36 * inch,
                             y_top - 0.65 * inch, "punched")

    # Total bar at the bottom
    total = sum(b["cost"] for b in bets)
    reserve = 300.00 - total
    total_y = top_y - len(bets) * bet_h - 0.05 * inch
    c.setFillColor(colors.HexColor("#1a1a2e"))
    c.rect(margin, total_y - 0.4 * inch, page_w - 2 * margin,
           0.4 * inch, fill=1, stroke=0)
    c.setFillColor(colors.HexColor("#ffd700"))
    c.setFont("Helvetica-Bold", 11)
    c.drawString(margin + 0.18 * inch, total_y - 0.18 * inch,
                 f"TOTAL DEPLOYED:  ${total:.2f}")
    c.setFillColor(colors.white)
    c.setFont("Helvetica", 9)
    c.drawString(margin + 2.6 * inch, total_y - 0.18 * inch,
                 f"+ ${reserve:.2f} reserve  =  $300.00 bankroll")
    c.setFillColor(colors.HexColor("#aaa"))
    c.setFont("Helvetica-Oblique", 7.5)
    c.drawString(margin + 0.18 * inch, total_y - 0.32 * inch,
                 "Reserve absorbs late line moves: if Right to Party drifts past 30-1 "
                 "or Crazy Mason's odds shift, hit them again with reserve cash.")

    c.save()
    print(f"[OK] {OUT}")
    print(f"     2 pages: order of finish (p1) + $300 bet sheet (p2)")


if __name__ == "__main__":
    main()
