"""Build a printable PDF betting sheet for the 2026 Kentucky Derby card.

Combines:
  - Pace-adjusted top picks
  - Win bet list
  - Dan-Style superfecta cascade (3 portfolios)
  - Pick 4 / Pick 5 / Pick 6 structures (multiple bankroll tiers)
  - Cross-card R7-R11 templates
  - Scratch-contingency replacement table

Output: C:/Users/danie/GRANDPA_JOE/data/derby_2026_betting_sheet.pdf
"""
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
)


REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "data" / "derby_2026_betting_sheet.pdf"


# Color palette
HEADER_BG = colors.HexColor("#1a1a2e")
ACCENT_BG = colors.HexColor("#16213e")
ROW_ALT = colors.HexColor("#f5f5f5")
GRID = colors.HexColor("#cccccc")
RED = colors.HexColor("#c1272d")
GREEN = colors.HexColor("#2d9c5e")


def make_styles():
    s = getSampleStyleSheet()
    s.add(ParagraphStyle(name="Title2", parent=s["Title"], fontSize=18,
                         textColor=HEADER_BG, spaceAfter=4))
    s.add(ParagraphStyle(name="Subhead", parent=s["Heading2"], fontSize=11,
                         textColor=ACCENT_BG, spaceAfter=2, spaceBefore=8))
    s.add(ParagraphStyle(name="BodyTight", parent=s["BodyText"], fontSize=9,
                         leading=11, spaceAfter=2))
    s.add(ParagraphStyle(name="Tag", parent=s["BodyText"], fontSize=8,
                         leading=10, textColor=colors.grey))
    return s


def title_block(s):
    return [
        Paragraph("KENTUCKY DERBY 2026 — BETTING SHEET", s["Title2"]),
        Paragraph(f"Saturday May 2, 2026 — Churchill Downs (post 6:57 ET)  "
                  f"·  Generated {datetime.now().strftime('%a %b %d %I:%M %p')} "
                  f"by Grandpa Joe (DJR/CAMDAN)", s["Tag"]),
        Spacer(1, 0.1 * inch),
    ]


def horses_block(s):
    title = Paragraph("FIELD &amp; KEY HORSES (post-DRF, pace-adjusted)", s["Subhead"])
    note = Paragraph(
        "<b>Pace shape: FAST</b> — 6 pacesetters with TFUS-Early ≥ 98 "
        "(Litmus Test 111, Potente 111, Further Ado 109, So Happy 107, Great White 107, Pavlovian 98). "
        "Deep closers + late-kick stalkers favored.",
        s["BodyTight"]
    )
    data = [
        ["PP", "Horse", "ML", "TFUS-E", "TFUS-L", "Joe% (adj)", "Edge", "Role"],
        ["6",  "Commandment",     "6-1",  "92",  "105", "20.0%", "+5.7", "★ KEY"],
        ["1",  "Renegade",        "4-1",  "73",  "116", "13.8%", "-6.2", "use"],
        ["9",  "The Puma",        "10-1", "91",  "104", "12.1%", "+3.0", "use"],
        ["8",  "So Happy",        "15-1", "107", "80",  "9.2%",  "+3.0", "use"],
        ["5",  "Right to Party",  "30-1", "70",  "108", "8.5%",  "+5.3", "★ PRICE"],
        ["19", "Golden Tempo",    "30-1", "57",  "113", "2.9%",  "-0.3", "bomb"],
        ["10", "Wonder Dean",     "30-1", "—",   "—",   "4.1%",  "+0.9", "bomb"],
        ["18", "Further Ado",     "6-1",  "109", "88",  "5.8%",  "-8.5", "TRAP"],
        ["12", "Chief Wallabee",  "8-1",  "98",  "104", "2.8%",  "-8.3", "TRAP"],
        ["14", "Potente",         "20-1", "111", "94",  "1.9%",  "-2.8", "TRAP"],
    ]
    t = Table(data, colWidths=[0.4*inch, 1.7*inch, 0.5*inch, 0.6*inch, 0.6*inch,
                                0.8*inch, 0.5*inch, 0.7*inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("ALIGN", (1, 0), (1, -1), "LEFT"),
        ("ALIGN", (-1, 0), (-1, -1), "LEFT"),
        ("GRID", (0, 0), (-1, -1), 0.5, GRID),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, ROW_ALT]),
        # Highlight the KEY and PRICE rows
        ("BACKGROUND", (0, 1), (-1, 1), colors.HexColor("#d4edda")),  # Commandment
        ("BACKGROUND", (0, 5), (-1, 5), colors.HexColor("#fff3cd")),  # Right to Party
        # Highlight TRAP rows
        ("BACKGROUND", (0, 8), (-1, 10), colors.HexColor("#f8d7da")),
    ]))
    return [title, note, Spacer(1, 0.05 * inch), t, Spacer(1, 0.1 * inch)]


def win_bets_block(s):
    title = Paragraph("WIN BETS — $30 (pace-adjusted overlays)", s["Subhead"])
    data = [
        ["#", "Bet", "Horse", "ML", "Stake", "Punched?"],
        ["1", "WIN R12", "#6 Commandment",     "6-1",  "$15", "☐"],
        ["2", "WIN R12", "#5 Right to Party",  "30-1", "$7",  "☐"],
        ["3", "WIN R12", "#9 The Puma",        "10-1", "$5",  "☐"],
        ["4", "WIN R12", "#8 So Happy",        "15-1", "$3",  "☐"],
    ]
    t = Table(data, colWidths=[0.3*inch, 0.8*inch, 1.8*inch, 0.6*inch,
                                0.7*inch, 0.8*inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("ALIGN", (-2, 0), (-2, -1), "RIGHT"),
        ("ALIGN", (-1, 0), (-1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.5, GRID),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, ROW_ALT]),
    ]))
    note = Paragraph(
        "<b>Do NOT bet Win on Renegade or Further Ado.</b> Renegade is chalk underlay (4-1), "
        "Further Ado is pace trap (TFUS-E 109 in 6-pace melt). Use them in 2nd/3rd slots only.",
        s["BodyTight"]
    )
    return [title, t, Spacer(1, 0.04*inch), note, Spacer(1, 0.1*inch)]


def cascade_block(s):
    title = Paragraph(
        "DAN-STYLE SUPERFECTA CASCADE — $277 (Derby R12)",
        s["Subhead"]
    )
    intro = Paragraph(
        "Three pace-scenario portfolios. Bullseye + tight straight + cascading wheels. "
        "Stake column shows base × combos = total.",
        s["BodyTight"]
    )
    blocks = []
    blocks.append(title)
    blocks.append(intro)
    blocks.append(Spacer(1, 0.04*inch))

    portfolios = [
        ("PORTFOLIO A — Commandment single key  (~$73)",
         [
             ["TRI",   "6 / 1 / 9",                                   "$5.00",  "1",   "$5.00",  "☐"],
             ["SUPER", "6 / 1 / 9 / 5",                               "$2.00",  "1",   "$2.00",  "☐"],
             ["TRI",   "6 / 1,9,5,8 / 1,9,5,8",                       "$1.00",  "12",  "$12.00", "☐"],
             ["SUPER", "6 / 1,9,5,8 / 1,9,5,8 / 1,9,5,8,18",          "$0.50",  "24",  "$12.00", "☐"],
             ["SUPER", "6 / 1,9,5 / 1,9,5,8,18 / 1,9,5,8,18,10,19",   "$0.10",  "105", "$10.50", "☐"],
             ["TRI",   "1 / 6,9,5,8 / 6,9,5,8,18  (Renegade alt key)","$0.50",  "16",  "$8.00",  "☐"],
             ["SUPER", "1 / 6,9,5,8 / 6,9,5,8,18 / 6,9,5,8,18,10,19", "$0.10",  "84",  "$8.40",  "☐"],
             ["TRI",   "5 / 6,1,9,8 / 6,1,9,8,18  (RTP top, 30-1)",   "$0.50",  "16",  "$8.00",  "☐"],
             ["SUPER", "5 / 6,1,9 / 6,1,9,8 / 6,1,9,8,18,10",         "$0.10",  "36",  "$3.60",  "☐"],
             ["SUPER", "6,1 / 6,1,9,5,8 / 6,1,9,5,8,18 / 6,1,9,5,8,18,10,19,11,15",  "$0.10", "224", "$22.40", "☐"],
         ]),
        ("PORTFOLIO B — Deep-closer meltdown jackpot  (~$139)",
         [
             ["TRI",   "1 / 6 / 5  (Renegade L116)",                  "$5.00",  "1",   "$5.00",  "☐"],
             ["SUPER", "1 / 6 / 5 / 19",                              "$2.00",  "1",   "$2.00",  "☐"],
             ["TRI",   "1 / 6,5,9,19,10 / 6,5,9,19,10",               "$1.00",  "20",  "$20.00", "☐"],
             ["SUPER", "1 / 6,5,9 / 6,5,9,19,10 / 6,5,9,19,10,8",     "$0.50",  "48",  "$24.00", "☐"],
             ["TRI",   "5 / 1,6,9,19 / 1,6,9,19,10  (RTP 30-1)",      "$0.50",  "16",  "$8.00",  "☐"],
             ["SUPER", "5 / 1,6,9 / 1,6,9,19 / 1,6,9,19,10,8",        "$0.10",  "36",  "$3.60",  "☐"],
             ["TRI",   "19 / 1,6,5,9 / 1,6,5,9,10  (Golden Tempo)",   "$0.50",  "16",  "$8.00",  "☐"],
             ["SUPER", "19 / 1,6,5 / 1,6,5,9 / 1,6,5,9,10,8",         "$0.10",  "36",  "$3.60",  "☐"],
             ["SUPER", "10 / 1,6,5 / 1,6,5,9,19 / 1,6,5,9,19,8 (W.Dean)", "$0.10","48", "$4.80",  "☐"],
             ["SUPER", "1,5,19,10 / 1,5,19,10,6,9 / 1,5,19,10,6,9,8 / 1,5,19,10,6,9,8", "$0.10","600","$60.00","☐"],
         ]),
        ("PORTFOLIO C — Stalker 1-2 (Cmdmt + Puma)  (~$65)",
         [
             ["TRI",   "6 / 9 / 1",                                   "$5.00",  "1",   "$5.00",  "☐"],
             ["SUPER", "6 / 9 / 1 / 5",                               "$2.00",  "1",   "$2.00",  "☐"],
             ["TRI",   "6,9 / 6,9 / 1,5,8,18",                        "$1.00",  "8",   "$8.00",  "☐"],
             ["SUPER", "6,9 / 6,9 / 1,5,8 / 1,5,8,18,19,10",          "$0.50",  "30",  "$15.00", "☐"],
             ["TRI",   "9 / 6,1,5,8 / 6,1,5,8,18  (Puma top)",        "$0.50",  "16",  "$8.00",  "☐"],
             ["SUPER", "9 / 6,1,5 / 6,1,5,8 / 6,1,5,8,18,10,19",      "$0.10",  "45",  "$4.50",  "☐"],
             ["SUPER", "8 / 6,1,9 / 6,1,9,5 / 6,1,9,5,18,10  (S.Happy)", "$0.10","36", "$3.60",  "☐"],
             ["SUPER", "6,9 / 6,9,1,5,8 / 6,9,1,5,8,18 / 6,9,1,5,8,18,10,19,11", "$0.10","192","$19.20","☐"],
         ]),
    ]

    for header, rows in portfolios:
        blocks.append(Paragraph(f"<b>{header}</b>", s["BodyTight"]))
        data = [["Type", "Structure", "Base", "Combos", "Cost", "✓"]] + rows
        t = Table(data, colWidths=[0.55*inch, 3.6*inch, 0.55*inch, 0.55*inch,
                                    0.6*inch, 0.3*inch])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), ACCENT_BG),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 7.8),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("ALIGN", (-3, 0), (-2, -1), "RIGHT"),
            ("ALIGN", (-1, 0), (-1, -1), "CENTER"),
            ("GRID", (0, 0), (-1, -1), 0.4, GRID),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, ROW_ALT]),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        blocks.append(t)
        blocks.append(Spacer(1, 0.06*inch))

    return blocks


def multirace_block(s):
    title = Paragraph(
        "MULTI-RACE BETS — Pick 4 / Pick 5 / Pick 6  (DERBY = LAST LEG)",
        s["Subhead"]
    )
    note = Paragraph(
        "<b>R7-R11 had no scratches as of Friday DRF pull.</b> Verify Saturday morning. "
        "Last-leg key for ALL multi-race bets: <b>6, 1, 9, 5</b> (Commandment / Renegade / Puma / RTP). "
        "Add 8 (So Happy) for wider wheels.",
        s["BodyTight"]
    )
    intro = [title, note, Spacer(1, 0.04*inch)]

    pick4_data = [
        ["Race", "Time ET", "Race Name (ML fav)", "My Horses", "Notes"],
        ["R9",  "4:06",  "American Turf S. (Stark Contrast 4/1)", "_____ , _____ , _____ ",
         "fill Sat AM"],
        ["R10", "4:50",  "Churchill Downs S. (Knightsbridge 9/5)", "_____ , _____ , _____ ",
         "fav-key OK"],
        ["R11", "5:39",  "Old Forester Bourbon Turf Cl. (Program Trading 4/1)",
         "_____ SINGLE", "single the chalk if confirmed"],
        ["R12", "6:57",  "KENTUCKY DERBY", "6, 1, 9, 5",
         "tight key (drop 8 if budget tight)"],
    ]
    pick4_para = Paragraph("<b>PICK 4 — R9-R12  (suggested $0.50 base, 36-60 combos = $18-30)</b>",
                            s["BodyTight"])
    pick4_table = Table(pick4_data, colWidths=[0.45*inch, 0.6*inch, 2.6*inch,
                                                 1.7*inch, 1.0*inch])
    pick4_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), ACCENT_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.4, GRID),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, ROW_ALT]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))

    pick5_data = [
        ["Race", "Time ET", "Race Name (ML fav)", "My Horses", "Notes"],
        ["R8",  "3:23",  "Pat Day Mile (Englishman 3/1)", "_____ , _____ , _____ ",
         "3 horses"],
        ["R9",  "4:06",  "American Turf",                  "_____ , _____ , _____ ", "3 horses"],
        ["R10", "4:50",  "Churchill Downs S.",             "_____ , _____ , _____ ", "3 horses"],
        ["R11", "5:39",  "Old Forester Bourbon Turf Cl.",  "_____ SINGLE",         "chalk single"],
        ["R12", "6:57",  "KENTUCKY DERBY",                 "6, 1, 9, 5",            "key 4"],
    ]
    pick5_para = Paragraph("<b>PICK 5 — R8-R12  (suggested $0.20 base, 108 combos = $21.60)</b>",
                            s["BodyTight"])
    pick5_table = Table(pick5_data, colWidths=[0.45*inch, 0.6*inch, 2.6*inch,
                                                 1.7*inch, 1.0*inch])
    pick5_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), ACCENT_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.4, GRID),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, ROW_ALT]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))

    pick6_data = [
        ["Race", "Time ET", "Race Name (ML fav)", "My Horses", "Notes"],
        ["R7",  "2:38",  "Distaff Turf Mile (Sweet Rebecca 5/2)", "_____ , _____ , _____ ",
         "3 horses"],
        ["R8",  "3:23",  "Pat Day Mile (Englishman 3/1)",        "_____ , _____ , _____ ", "3 horses"],
        ["R9",  "4:06",  "American Turf",                         "_____ , _____ , _____ ", "3 horses"],
        ["R10", "4:50",  "Churchill Downs S.",                    "_____ , _____ , _____ ", "3 horses"],
        ["R11", "5:39",  "Old Forester Bourbon Turf Cl.",         "_____ SINGLE",          "single"],
        ["R12", "6:57",  "KENTUCKY DERBY",                        "6, 1, 9",                "TIGHT 3"],
    ]
    pick6_para = Paragraph(
        "<b>PICK 6 ULTRA-TIGHT — R7-R12  (suggested $0.20 base, 243 combos = $48.60)</b><br/>"
        "<i>Only play this tier if you can confirm an R11 chalk single. Otherwise drop to Pick 5.</i>",
        s["BodyTight"]
    )
    pick6_table = Table(pick6_data, colWidths=[0.45*inch, 0.6*inch, 2.6*inch,
                                                 1.7*inch, 1.0*inch])
    pick6_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), ACCENT_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.4, GRID),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, ROW_ALT]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))

    return intro + [pick4_para, pick4_table, Spacer(1, 0.06*inch),
                    pick5_para, pick5_table, Spacer(1, 0.06*inch),
                    pick6_para, pick6_table, Spacer(1, 0.1*inch)]


def scratch_contingency_block(s):
    title = Paragraph(
        "SCRATCH CONTINGENCY — replacement plan if more scratches Saturday morning",
        s["Subhead"]
    )
    note = Paragraph(
        "Most tracks let you swap a scratched horse with the morning-line favorite "
        "automatically. <b>Confirm with the teller</b> when punching the ticket.",
        s["BodyTight"]
    )
    data = [
        ["If THIS scratches", "Replace with", "Why", "Tickets affected"],
        ["#6 Commandment (top key)",
         "★ #1 Renegade promotes to KEY; #9 The Puma slides to part",
         "Renegade is alt key; pace-adj #2",
         "ALL Portfolio A, all Win bets, all P4/P5/P6"],
        ["#1 Renegade (alt key)",
         "Drop from key; bump #9 The Puma into 2nd-slot wheel",
         "Renegade is the chalk alt; lose him = lose the chalk",
         "Portfolio A Tier 4, all Win cascades"],
        ["#5 Right to Party (price play)",
         "★ #19 Golden Tempo (TFUS-L 113) takes the deep-closer slot",
         "Both deep closers in melt scenario",
         "Portfolio B, $7 Win bet"],
        ["#9 The Puma (your read)",
         "Bump #15 Emerging Market into the stalker slot",
         "Brown/Prat, similar profile",
         "Portfolio C, $5 Win bet"],
        ["#8 So Happy",
         "Drop from tickets entirely (was a borderline overlay)",
         "Pace haircut already small; not worth replacing",
         "$3 Win bet, supers wider slot only"],
        ["#19 Golden Tempo (bomb)",
         "★ #10 Wonder Dean (Japan deep closer) takes the slot",
         "Same closer profile, similar price",
         "Portfolio B bombs"],
    ]
    t = Table(data, colWidths=[1.7*inch, 2.0*inch, 1.6*inch, 1.5*inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.4, GRID),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, ROW_ALT]),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
    ]))
    return [title, note, Spacer(1, 0.04*inch), t, Spacer(1, 0.1*inch)]


def rules_block(s):
    title = Paragraph("RULES OF THE DAY", s["Subhead"])
    items = [
        "<b>1.</b> Commandment is the KEY — single him on top in tris/supers. Pace-adj #1.",
        "<b>2.</b> Right to Party (30-1) is the PRICE play — don't skimp. Bombs hit in pace meltdowns.",
        "<b>3.</b> NO Win bet on Renegade or Further Ado. Use Renegade in 2nd/3rd. Drop Further Ado to bottom slots only.",
        "<b>4.</b> Verify R7-R11 entries Saturday morning before punching Pick 4/5/6. R7-R11 had no scratches Friday but check again.",
        "<b>5.</b> No bets after the gates load. If you're refreshing odds in the last 2 minutes, you're done betting.",
        "<b>6.</b> Reserve $20+ for late line moves. If RTP drifts past 30-1, hammer him again.",
        "<b>7.</b> Tomorrow morning: re-run the model with the latest scratches before printing the FINAL sheet.",
    ]
    paras = [title]
    for it in items:
        paras.append(Paragraph(it, s["BodyTight"]))
    return paras


def main():
    s = make_styles()
    doc = SimpleDocTemplate(
        str(OUT),
        pagesize=letter,
        leftMargin=0.4*inch, rightMargin=0.4*inch,
        topMargin=0.4*inch, bottomMargin=0.4*inch,
        title="Derby 2026 Betting Sheet",
        author="Grandpa Joe (DJR/CAMDAN)",
    )

    story = []
    story.extend(title_block(s))
    story.extend(horses_block(s))
    story.extend(win_bets_block(s))
    story.append(PageBreak())
    story.extend(cascade_block(s))
    story.append(PageBreak())
    story.extend(multirace_block(s))
    story.extend(scratch_contingency_block(s))
    story.extend(rules_block(s))

    doc.build(story)
    print(f"[OK] Built {OUT}")
    print(f"     Pages: open in any PDF viewer; portrait letter size")


if __name__ == "__main__":
    main()
