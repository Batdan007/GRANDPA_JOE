"""Build the Oaks Day master PDF (Friday May 1, 2026).

Page 1: Predicted order of finish for all 13 races on the Oaks Day card.
Page 2: $300 bet sheet — Win + Exotics on the Oaks (R13) + Pick 4/5 leading in.

Mutuel-ticket-style format with race numbers and post numbers in punch order.

Output: data/OAKS_DAY_2026_MASTER.pdf
"""
import json
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"
PICKS = DATA / "oaks_day_picks.json"
OUT = DATA / "OAKS_DAY_2026_MASTER.pdf"

OAKS_PINK = colors.HexColor("#9d2235")  # Kentucky Oaks signature pink/maroon


def main():
    with open(PICKS) as f:
        d = json.load(f)
    races = sorted([r for r in d["races"] if r["horses"]],
                    key=lambda r: r["race"])

    page_w, page_h = letter
    margin = 0.3 * inch
    c = canvas.Canvas(str(OUT), pagesize=letter)

    # ── PAGE 1: Order of finish ────────────────────────────────────────
    # Title
    c.setFillColor(OAKS_PINK)
    c.rect(0, page_h - 0.55 * inch, page_w, 0.55 * inch, fill=1, stroke=0)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 16)
    c.drawCentredString(page_w / 2, page_h - 0.3 * inch,
                        "OAKS DAY 2026 - ALL RACES PREDICTED ORDER OF FINISH")
    c.setFont("Helvetica", 9)
    c.drawCentredString(page_w / 2, page_h - 0.46 * inch,
                        f"Churchill Downs - Friday May 1, 2026 - "
                        f"Generated {datetime.now().strftime('%a %b %d %I:%M %p')}")

    # 2-column layout for 13 races
    col_w = (page_w - 3 * margin) / 2
    cl_x = margin
    cr_x = margin * 2 + col_w
    top_y = page_h - 0.75 * inch
    n_rows = (len(races) + 1) // 2  # 7 in left col, 6 in right
    block_h = (top_y - 0.95 * inch) / n_rows

    for idx, r in enumerate(races):
        col = 0 if idx < n_rows else 1
        row = idx if idx < n_rows else idx - n_rows
        x = cl_x if col == 0 else cr_x
        y_top = top_y - row * block_h

        conf = r.get("single_confidence", "WIDE")
        bar = (colors.HexColor("#28a745") if conf == "STRONG"
               else colors.HexColor("#ffc107") if conf == "MEDIUM"
               else colors.HexColor("#6c757d"))

        c.setFillColor(bar)
        c.rect(x, y_top - 0.22 * inch, col_w, 0.22 * inch, fill=1, stroke=0)
        c.setFillColor(colors.white if conf != "MEDIUM" else colors.black)
        c.setFont("Helvetica-Bold", 10)
        race_label = f"R{r['race']}"
        if r["race"] == 13:
            race_label = "R13 - KENTUCKY OAKS (G1)"
        c.drawString(x + 0.06 * inch, y_top - 0.155 * inch, race_label)

        c.setFont("Helvetica", 8)
        right_text = f"pace: {r.get('pace_shape','?')}  |  {conf}"
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
            name = h["name"][:22] + ("..." if len(h["name"]) > 22 else "")
            c.drawString(x + 0.7 * inch, row_y, name)
            c.setFont("Helvetica", 8)
            c.setFillColor(colors.HexColor("#666"))
            c.drawRightString(x + col_w - 0.1 * inch, row_y, h.get("ml") or "—")

    # Footer: legend + singles + commands (~1 inch)
    c.setStrokeColor(colors.HexColor("#1a1a2e"))
    c.setLineWidth(1.2)
    c.line(margin, 0.92 * inch, page_w - margin, 0.92 * inch)

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
        meaning = {"SINGLE": "= SINGLE",
                   "MED": "= 2 horses",
                   "WIDE": "= 3-4 horses"}[lbl]
        c.drawString(cur_x, 0.71 * inch, meaning)
        cur_x += c.stringWidth(meaning, "Helvetica", 8.5) + 0.22 * inch

    # Singling candidates
    singles = [r for r in races if r.get("single_confidence") == "STRONG"]
    if singles:
        c.setFillColor(colors.HexColor("#28a745"))
        c.setFont("Helvetica-Bold", 8.5)
        c.drawString(margin + 0.05 * inch, 0.50 * inch, "SINGLES TODAY:")
        c.setFillColor(colors.HexColor("#333"))
        c.setFont("Helvetica", 8)
        single_text = "  -  ".join(
            f"R{r['race']} #{r['horses'][0]['post']} {r['horses'][0]['name']}"
            for r in singles
        )
        c.drawString(margin + 1.0 * inch, 0.50 * inch, single_text[:160])

    # Rebuild commands
    cmd_y = 0.18 * inch
    c.setFillColor(colors.HexColor("#0f0f23"))
    c.rect(margin, cmd_y, page_w - 2 * margin, 0.22 * inch, fill=1, stroke=0)
    c.setFillColor(colors.HexColor("#ffd700"))
    c.setFont("Helvetica-Bold", 7.5)
    c.drawString(margin + 0.06 * inch, cmd_y + 0.13 * inch, "REBUILD (PS):")
    c.setFillColor(colors.HexColor("#a0e0ff"))
    c.setFont("Courier-Bold", 7.5)
    c.drawString(margin + 1.0 * inch, cmd_y + 0.13 * inch,
                 "git pull origin main; python scripts\\process_oaks_card.py; "
                 "python scripts\\fix_oaks_r13.py; python scripts\\build_oaks_master.py")
    c.setFillColor(colors.HexColor("#888"))
    c.setFont("Helvetica", 6.5)
    c.drawString(margin + 0.06 * inch, cmd_y + 0.03 * inch,
                 "From C:\\Users\\danie\\GRANDPA_JOE  -  refreshes order of finish + Oaks bet sheet")

    # ── PAGE 2: $300 BET SHEET FOR OAKS DAY ────────────────────────────
    c.showPage()
    c.setFillColor(OAKS_PINK)
    c.rect(0, page_h - 0.55 * inch, page_w, 0.55 * inch, fill=1, stroke=0)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 16)
    c.drawCentredString(page_w / 2, page_h - 0.3 * inch,
                        "$300 OAKS DAY BET SHEET - May 1, 2026")
    c.setFont("Helvetica", 9)
    c.drawCentredString(page_w / 2, page_h - 0.46 * inch,
                        "1 EXA + 1 TRI + 1 SUP for the Oaks (R13) + Pick 4 + Pick 5")

    # Get top horses for Oaks Day legs (R10, R11, R12, R13)
    by_num = {r["race"]: r for r in races}
    def top_n(race_num, n):
        r = by_num.get(race_num)
        return [(h["post"], h["name"]) for h in r["horses"][:n]] if r else []

    r10 = top_n(10, 2)  # MEDIUM gap, use 2
    r11 = top_n(11, 1)  # STRONG single Drexel Hill (gap 18.9)
    r12 = top_n(12, 3)
    r13 = top_n(13, 4)  # Always a Runner / Lovely Grey / Zany / Resist

    # Bet definitions
    # Oaks-Derby Double: blend model picks (Oaks) with model + chalk for Derby leg
    # Oaks 4: 9 Always a Runner, 2 Zany, 10 Prom Queen, 11 Percy's Bar
    # Derby 4: 6 Commandment, 1 Renegade, 9 Puma, 19 Golden Tempo
    oaks_dbl_legs = "9, 2, 10, 11"
    derby_dbl_legs = "1, 6, 9, 19"

    bets = [
        ("OAKS-DERBY DOUBLE", "R13 today + R12 tomorrow",
         f"$2 OAKS-DERBY DOUBLE  {oaks_dbl_legs}  /  {derby_dbl_legs}",
         f'"$2 Oaks-Derby double, Oaks 2-9-10-11 with Derby 1-6-9-19"',
         "Oaks: 9 Always a Runner / 2 Zany / 10 Prom Queen / 11 Percy's Bar  -  "
         "Derby: 1 Renegade / 6 Cmdmt / 9 Puma / 19 Golden Tempo",
         "16 combos. Special 2-day wager pays premium over separate Wins. "
         "PUNCH BEFORE THE OAKS GOES OFF (5:51 ET).",
         32.00),
        ("OAKS EXACTA", "R13 (5:51 ET)",
         f"$5 EXACTA BOX  {r13[0][0]}, {r13[1][0] if len(r13)>1 else '?'}   (race 13)",
         f'"$5 exacta box, {r13[0][0]} and {r13[1][0] if len(r13)>1 else "?"}, race thirteen"',
         f"  {r13[0][0]} = {r13[0][1]}   |   {r13[1][0] if len(r13)>1 else '?'} = {r13[1][1] if len(r13)>1 else '?'}",
         "Model's top 2 in the Oaks; either order cashes the box.",
         10.00),
        ("OAKS TRIFECTA", "R13",
         f"$2 TRIFECTA  {r13[0][0]} / {r13[1][0]}, {r13[2][0]} / {r13[1][0]}, {r13[2][0]}, {r13[3][0]}   (race 13)",
         f'"$2 trifecta, KEY {r13[0][0]}, 2nd {r13[1][0]} {r13[2][0]}, 3rd {r13[1][0]} {r13[2][0]} {r13[3][0]}, race thirteen"',
         f"  {r13[0][0]} {r13[0][1]} KEY  -  {r13[1][0]} {r13[1][1]}  -  {r13[2][0]} {r13[2][1]}  -  {r13[3][0]} {r13[3][1]}",
         "Model #1 KEY over the next 3. Always a Runner / Lovely Grey / Zany live cover.",
         12.00),
        ("OAKS SUPERFECTA", "R13",
         f"$1 SUPER  {r13[0][0]} / {r13[1][0]}, {r13[2][0]}, {r13[3][0]} / {r13[1][0]}, {r13[2][0]}, {r13[3][0]} / "
         f"{r13[1][0]}, {r13[2][0]}, {r13[3][0]}",
         f'"$1 super, {r13[0][0]} with {r13[1][0]} {r13[2][0]} {r13[3][0]}, with {r13[1][0]} {r13[2][0]} {r13[3][0]}, with same, race thirteen"',
         f"  KEY {r13[0][0]} {r13[0][1]} on top with the next 3 anywhere",
         "Cheap super under the model's #1 — only 6 valid combos.",
         6.00),
        ("PICK 4 (R10-R13)", "Late P4 into Oaks",
         f"$2 PICK 4  R10: {','.join(str(p) for p,_ in r10)}  "
         f"R11: {','.join(str(p) for p,_ in r11)} (SINGLE)  "
         f"R12: {','.join(str(p) for p,_ in r12)}  "
         f"R13: {','.join(str(p) for p,_ in r13)}",
         f'"$2 pick 4: race 10 {", ".join(str(p) for p,_ in r10)}; race 11 single {r11[0][0] if r11 else "?"}; '
         f'race 12 {", ".join(str(p) for p,_ in r12)}; race 13 {", ".join(str(p) for p,_ in r13)}"',
         f"R10: {' / '.join(n for _,n in r10)} | R11: {r11[0][1] if r11 else '?'} (SINGLE) | "
         f"R12: {' / '.join(n for _,n in r12)} | R13: {' / '.join(n for _,n in r13)}",
         "Singling R11 (gap 18.9) collapses combos. R10 MEDIUM (2), R12 WIDE (3), R13 (4).",
         48.00),
        ("PICK 5 (R9-R13)", "5-leg sequence",
         f"$1 PICK 5  R9: {','.join(str(p) for p,_ in top_n(9, 3))}  "
         f"R10: {','.join(str(p) for p,_ in r10)}  "
         f"R11: {r11[0][0] if r11 else '?'} (SINGLE)  "
         f"R12: {','.join(str(p) for p,_ in r12)}  "
         f"R13: {','.join(str(p) for p,_ in r13)}",
         f'"$1 pick 5: race 9 numbers; race 10 numbers; race 11 single; race 12 numbers; race 13 numbers"',
         f"R9: {' / '.join(n for _,n in top_n(9, 3))} | R10-R13 same as P4",
         "Adds R9 leg with 3 horses. Total 72 combos at $1 base.",
         72.00),
        ("OAKS WIN BET", "R13",
         f"$30 WIN  #{r13[0][0]} {r13[0][1]}   (race 13)",
         f'"$30 to win on number {r13[0][0]}, race thirteen"',
         f"  Single Win bet on the model's pace-adjusted #1.",
         "Always a Runner has TFUS-L 110 (best late kick in the field). "
          "Public chalk Zany at 4-1 is value-light; this is the priced overlay at 10-1.",
         30.00),
    ]

    top_y = page_h - 0.75 * inch
    bet_h = 1.05 * inch
    bet_x = margin
    bet_w = page_w - 2 * margin

    for i, (name, race_label, ticket, say, horses, why, cost) in enumerate(bets):
        y_top = top_y - i * bet_h

        # alt row stripe
        if i % 2 == 0:
            c.setFillColor(colors.HexColor("#f8f9fa"))
        else:
            c.setFillColor(colors.HexColor("#ffffff"))
        c.rect(bet_x, y_top - bet_h + 0.05 * inch, bet_w,
               bet_h - 0.06 * inch, fill=1, stroke=0)

        # number badge
        c.setFillColor(OAKS_PINK)
        c.circle(bet_x + 0.32 * inch, y_top - 0.22 * inch,
                 0.18 * inch, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 14)
        c.drawCentredString(bet_x + 0.32 * inch, y_top - 0.27 * inch, str(i + 1))

        # name + race
        c.setFillColor(colors.HexColor("#1a1a2e"))
        c.setFont("Helvetica-Bold", 11)
        c.drawString(bet_x + 0.6 * inch, y_top - 0.20 * inch, name)
        c.setFillColor(colors.HexColor("#666"))
        c.setFont("Helvetica", 8)
        c.drawString(bet_x + 0.6 * inch, y_top - 0.34 * inch, race_label)

        # cost
        c.setFillColor(colors.HexColor("#28a745"))
        c.setFont("Helvetica-Bold", 16)
        c.drawRightString(bet_x + bet_w - 0.15 * inch,
                          y_top - 0.27 * inch, f"${cost:.2f}")

        # ticket
        c.setFillColor(colors.HexColor("#fff8dc"))
        c.rect(bet_x + 0.15 * inch, y_top - 0.55 * inch,
               bet_w - 0.3 * inch, 0.20 * inch, fill=1, stroke=0)
        c.setStrokeColor(colors.HexColor("#ffc107"))
        c.rect(bet_x + 0.15 * inch, y_top - 0.55 * inch,
               bet_w - 0.3 * inch, 0.20 * inch, fill=0, stroke=1)
        c.setFillColor(colors.HexColor("#0f0f23"))
        c.setFont("Helvetica-Bold", 8)
        c.drawString(bet_x + 0.22 * inch, y_top - 0.42 * inch, "TICKET:")
        c.setFont("Courier-Bold", 8.5)
        c.drawString(bet_x + 0.78 * inch, y_top - 0.42 * inch, ticket[:140])

        # say
        c.setFillColor(colors.HexColor("#1a1a2e"))
        c.setFont("Helvetica-Bold", 7.5)
        c.drawString(bet_x + 0.22 * inch, y_top - 0.70 * inch, "SAY:")
        c.setFillColor(colors.HexColor("#333"))
        c.setFont("Helvetica-Oblique", 7.5)
        c.drawString(bet_x + 0.55 * inch, y_top - 0.70 * inch, say[:140])

        # horses
        c.setFillColor(colors.HexColor("#1a1a2e"))
        c.setFont("Helvetica-Bold", 7.5)
        c.drawString(bet_x + 0.22 * inch, y_top - 0.85 * inch, "HORSES:")
        c.setFillColor(colors.black)
        c.setFont("Helvetica", 7.5)
        c.drawString(bet_x + 0.78 * inch, y_top - 0.85 * inch, horses[:140])

        # why
        c.setFillColor(colors.HexColor("#444"))
        c.setFont("Helvetica-Oblique", 7.5)
        c.drawString(bet_x + 0.22 * inch, y_top - 0.99 * inch,
                     "WHY: " + why[:160])

        # punched check
        c.setStrokeColor(colors.HexColor("#1a1a2e"))
        c.setLineWidth(1.2)
        c.rect(bet_x + bet_w - 0.5 * inch, y_top - 0.50 * inch,
               0.18 * inch, 0.18 * inch, fill=0, stroke=1)
        c.setFillColor(colors.HexColor("#666"))
        c.setFont("Helvetica", 6.5)
        c.drawCentredString(bet_x + bet_w - 0.41 * inch,
                             y_top - 0.60 * inch, "punched")

    # Total bar
    total = sum(b[6] for b in bets)
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
                 f"+ ${300.00 - total:.2f} reserve  =  $300.00 bankroll")

    c.save()
    print(f"[OK] {OUT}")
    print(f"     2 pages: order of finish (p1) + $300 Oaks Day bet sheet (p2)")
    print(f"     Total deployed: ${total:.2f}")


if __name__ == "__main__":
    main()
