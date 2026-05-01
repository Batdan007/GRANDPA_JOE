"""Full Derby Day card processor — parses the TFUS Pace Projector PDF for
every race on Churchill May 2, ranks each race with the pace overlay model,
and identifies the best races to SINGLE in Pick 4/5/6 sequences.

Inputs:
  data/CD--TFUS-05-02-2026.pdf  (TFUS Pace Projector, all races)
  data/CD--FULL-05-02-2026.pdf  (DRF PPs, all races — used for trainer/jockey)
  data/derby_2026_pps.json       (Derby R12 source of truth — scratches + AEs)

Outputs:
  data/full_card_picks.json
  data/full_card_order_of_finish.pdf
  data/full_card_pick456_strategy.pdf
  bundle/race_3d/CD_R{n}_2026-05-02_animated.html  (per race)
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
    import fitz
except ImportError:
    print("[ERR] pip install pymupdf")
    sys.exit(1)


TFUS_PDF = DATA / "CD--TFUS-05-02-2026.pdf"
DRF_FULL_PDF = DATA / "CD--FULL-05-02-2026.pdf"
DERBY_PPS = DATA / "derby_2026_pps.json"


# ── TFUS PDF parser ────────────────────────────────────────────────────
RACE_HEADER_RE = re.compile(r"^RACE\s*\n?\s*(\d+)\s*$", re.MULTILINE)
HORSE_LINE_RE = re.compile(
    r"^(\d{1,2})\s+([A-Z][A-Za-z' ()/.-]+?)\s*\n"
    r"ML:\s*([0-9/-]+|EVEN|MTO)",
    re.MULTILINE
)
RUNNING_STYLE_RE = re.compile(
    r"Running Style:\s*(\d+)\s+(Early|Mid Pace|Midpack|Mid|Stalker|Presser|Closer|Pace Pres|Pace|Pacesetter)\s+(\d+)",
    re.IGNORECASE
)
POWER_PICKS_RE = re.compile(
    r"Power Picks\s*\n((?:\s*[A-Z][A-Za-z' ()-]+\s*\n){1,5})",
    re.MULTILINE
)
RACE_INFO_RE = re.compile(
    r"(\d{1,2}:\d{2}\s*[AP]M).*?\n.*?\$(\d+(?:[.,]\d+)?[KM]?)\s*\n.*?(\d+\s*[\d/]*\s*[MF]\.?)",
    re.DOTALL
)


def normalize_style(label: str) -> str:
    """Map TFUS style labels to our internal categories."""
    l = label.lower().replace(" ", "")
    if "pace" in l and "pres" in l: return "presser"
    if l in ("early", "pacesetter"): return "pacesetter"
    if l in ("presser", "pacepres"): return "presser"
    if l in ("mid", "midpack", "midpace", "stalker"): return "stalker"
    if l == "closer": return "closer"
    return "stalker"


def parse_tfus_pdf(pdf_path: Path) -> list:
    """Parse the TFUS Pace Projector PDF — returns list of race dicts."""
    doc = fitz.open(pdf_path)
    full_text_pages = [p.get_text() for p in doc]
    full = "\n".join(full_text_pages)

    # Find every RACE\n{N} boundary marker
    boundaries = []
    for m in re.finditer(r"\bRACE\s*\n\s*(\d+)\s+Churchill Downs", full):
        boundaries.append((m.start(), int(m.group(1))))

    races = []
    for i, (pos, race_num) in enumerate(boundaries):
        end = boundaries[i + 1][0] if i + 1 < len(boundaries) else len(full)
        text = full[pos:end]
        race = parse_one_race_tfus(race_num, text)
        if race:
            races.append(race)
    return races


def parse_one_race_tfus(race_num: int, text: str) -> dict:
    """Parse one race's TFUS chunk."""
    horses = []

    # Extract Power Picks (top of each race)
    pp_match = POWER_PICKS_RE.search(text)
    power_picks = []
    if pp_match:
        for ln in pp_match.group(1).strip().split("\n"):
            ln = ln.strip()
            if ln and not ln.startswith("PACE"):
                power_picks.append(ln)

    # Extract race meta from first ~500 chars
    head = text[:600]
    post_time = None
    purse = None
    distance = None
    surface = "dirt"
    pt = re.search(r"(\d{1,2}:\d{2}\s*[AP]M)", head)
    if pt: post_time = pt.group(1)
    pu = re.search(r"\$(\d+(?:[.,]\d+)?[KM])", head)
    if pu: purse = "$" + pu.group(1)
    di = re.search(r"(\d+(?:\s*\d/\d+)?\s*M(?:I?LE?S?)?|\d+\s*F)", head, re.IGNORECASE)
    if di: distance = di.group(1).strip()
    if "Turf" in head or "turf" in head:
        surface = "turf"
    elif "Synth" in head or "synth" in head:
        surface = "synth"

    # Walk the text, find each horse block
    # Pattern: a line that's just a number (1-20) followed by horse name
    # then "ML: X-Y | | WT: NN" then "Equipment:" then "Running Style: E STYLE L"
    horse_blocks = re.split(r"\n(\d{1,2})\s+([A-Z][A-Za-z' ()/.-]+?)\n(?=ML:)", text)
    # horse_blocks: [preamble, post1, name1, body1, post2, name2, body2, ...]
    for i in range(1, len(horse_blocks), 3):
        try:
            post = int(horse_blocks[i])
        except ValueError:
            continue
        name = horse_blocks[i + 1].strip()
        body = horse_blocks[i + 2]

        # ML
        ml_m = re.search(r"ML:\s*([0-9/-]+|EVEN|MTO)", body)
        ml = ml_m.group(1) if ml_m else None

        # Running style
        rs_m = RUNNING_STYLE_RE.search(body)
        if rs_m:
            tfus_e = int(rs_m.group(1))
            tfus_l = int(rs_m.group(3))
            style = normalize_style(rs_m.group(2))
        else:
            tfus_e = None
            tfus_l = None
            style = "stalker"

        # Trainer (line after "Owner:")
        tr_m = re.search(r"^([A-Z]\.\s*[A-Z][A-Za-z]+)\s*\n", body, re.MULTILINE)
        trainer = tr_m.group(1) if tr_m else None

        # Jockey (line "Jockey L. Lastname" in ratings table)
        jk_m = re.search(r"Jockey\s+([A-Z]\.\s*[A-Z][A-Za-z']+)", body)
        jockey = jk_m.group(1) if jk_m else None

        # Top fig (Beyer-equivalent)
        tf_m = re.search(r"\$[\d,]+\s*\n(\d+)\s*\n", body)
        top_fig = int(tf_m.group(1)) if tf_m else None

        horses.append({
            "post": post,
            "name": name,
            "ml": ml,
            "tfus_early": tfus_e,
            "tfus_late": tfus_l,
            "style": style,
            "trainer": trainer,
            "jockey": jockey,
            "top_fig": top_fig,
        })

    return {
        "race": race_num,
        "post_time": post_time,
        "distance": distance,
        "surface": surface,
        "purse": purse,
        "power_picks": power_picks,
        "horses": horses,
    }


# ── Pace-overlay ranking ───────────────────────────────────────────────
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
    n_pace = sum(1 for h in horses if h["style"] == "pacesetter"
                 or (h["tfus_early"] and h["tfus_early"] >= 100))

    for h in horses:
        late = h["tfus_late"] or 90
        early = h["tfus_early"] or 90
        top_fig = h["top_fig"] or 90
        # Score: TFUS Late + small Top-fig bonus + low-Early bonus
        base = late * 1.0 + max(0, 95 - early) * 0.3 + (top_fig - 90) * 0.5
        h["pace_factor"] = pace_factor(early, late, n_pace)
        h["score"] = base * h["pace_factor"]
        # Power-pick bonus: DRF flagged this horse
        if h["name"] in race.get("power_picks", []):
            h["score"] *= 1.05  # 5% bump for DRF power pick
            h["power_pick"] = True
        else:
            h["power_pick"] = False

    horses.sort(key=lambda h: -h["score"])

    # Compute confidence gaps
    if len(horses) >= 2:
        race["gap_1_2"] = horses[0]["score"] - horses[1]["score"]
        race["gap_1_3"] = horses[0]["score"] - horses[2]["score"] if len(horses) >= 3 else 0
        # Singling confidence: bigger gap = more confident single
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


# ── Pick 4/5/6 ticket strategy ──────────────────────────────────────────
def build_ticket_strategy(races: list) -> dict:
    """Given the ranked races, propose Pick 4/5/6 structures based on confidence gaps."""

    by_num = {r["race"]: r for r in races}

    def horses_for_leg(race, target_horses=3):
        """Pick the top N horses for a leg, sized by confidence."""
        n = len(race["horses"])
        if n == 0:
            return []
        conf = race["single_confidence"]
        if conf == "STRONG":
            count = 1
        elif conf == "MEDIUM":
            count = 2
        else:
            count = min(target_horses, max(3, int(n * 0.3)))
        return [h["name"] for h in race["horses"][:count]]

    strategies = {}

    # Pick 4 (R9-R12)
    p4_legs = [9, 10, 11, 12]
    if all(n in by_num for n in p4_legs):
        legs = [(n, horses_for_leg(by_num[n])) for n in p4_legs]
        # Force Derby leg to match our pace-adjusted top (post-RtP scratch)
        legs[-1] = (12, ["Commandment", "Renegade", "The Puma", "Golden Tempo"])
        combos = 1
        for _, hs in legs:
            combos *= len(hs)
        strategies["pick4"] = {
            "races": p4_legs,
            "legs": legs,
            "combos": combos,
            "cost_50c": combos * 0.50,
            "cost_1": combos * 1.00,
        }

    # Pick 5 (R8-R12)
    p5_legs = [8, 9, 10, 11, 12]
    if all(n in by_num for n in p5_legs):
        legs = [(n, horses_for_leg(by_num[n])) for n in p5_legs]
        legs[-1] = (12, ["Commandment", "Renegade", "The Puma", "Golden Tempo"])
        combos = 1
        for _, hs in legs:
            combos *= len(hs)
        strategies["pick5"] = {
            "races": p5_legs,
            "legs": legs,
            "combos": combos,
            "cost_20c": combos * 0.20,
            "cost_50c": combos * 0.50,
        }

    # Pick 6 (R7-R12)
    p6_legs = [7, 8, 9, 10, 11, 12]
    if all(n in by_num for n in p6_legs):
        legs = [(n, horses_for_leg(by_num[n])) for n in p6_legs]
        legs[-1] = (12, ["Commandment", "Renegade", "The Puma"])  # tighter Derby for P6
        combos = 1
        for _, hs in legs:
            combos *= len(hs)
        strategies["pick6"] = {
            "races": p6_legs,
            "legs": legs,
            "combos": combos,
            "cost_10c": combos * 0.10,
            "cost_20c": combos * 0.20,
        }

    return strategies


# ── PDF output ──────────────────────────────────────────────────────────
def write_full_card_pdf(races: list, strategies: dict, out: Path):
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                     Table, TableStyle, PageBreak)
    s = getSampleStyleSheet()
    s.add(ParagraphStyle(name="Sub", parent=s["Heading2"], fontSize=11,
                         textColor=colors.HexColor("#16213e"),
                         spaceAfter=2, spaceBefore=8))
    s.add(ParagraphStyle(name="Tight", parent=s["BodyText"], fontSize=8.5,
                         leading=11, spaceAfter=2))

    doc = SimpleDocTemplate(str(out), pagesize=letter,
                             leftMargin=0.4 * inch, rightMargin=0.4 * inch,
                             topMargin=0.4 * inch, bottomMargin=0.4 * inch)
    story = []

    story.append(Paragraph(
        "<b>CHURCHILL DOWNS — DERBY DAY 2026</b><br/>"
        "<font size=10>Full-card order of finish + Pick 4/5/6 ticket strategy</font>",
        s["Title"]))
    story.append(Spacer(1, 0.15 * inch))

    # Table of contents w/ confidence flags
    toc = [["Race", "Horses", "Pace", "#1 (gap)", "Confidence", "DRF Power"]]
    for r in races:
        h = r["horses"]
        if not h:
            continue
        gap = r.get("gap_1_2", 0)
        toc.append([
            f"R{r['race']}",
            str(len(h)),
            r.get("pace_shape", "?"),
            f"{h[0]['name']} ({gap:.1f})",
            r.get("single_confidence", "?"),
            ", ".join(r.get("power_picks", [])[:2]) or "—",
        ])
    t = Table(toc, colWidths=[0.4*inch, 0.5*inch, 0.6*inch, 2.4*inch,
                               0.9*inch, 2.5*inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1a2e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, colors.HexColor("#f5f5f5")]),
    ]))
    # Color-code confidence column
    for i, r in enumerate(races, 1):
        c = r.get("single_confidence")
        bg = None
        if c == "STRONG":
            bg = colors.HexColor("#d4edda")  # green
        elif c == "MEDIUM":
            bg = colors.HexColor("#fff3cd")  # yellow
        elif c == "WIDE":
            bg = colors.HexColor("#f8d7da")  # red
        if bg:
            t.setStyle(TableStyle([
                ("BACKGROUND", (4, i), (4, i), bg),
            ]))
    story.append(Paragraph("<b>FULL CARD AT A GLANCE</b>", s["Sub"]))
    story.append(Paragraph(
        "<font size=8 color=grey>Green = SINGLE this leg; Yellow = use 2 horses; Red = wider 3-4 horses</font>",
        s["Tight"]))
    story.append(t)
    story.append(Spacer(1, 0.15 * inch))

    # Pick 4/5/6 strategies
    story.append(Paragraph("<b>PICK 4 / PICK 5 / PICK 6 STRATEGY</b>", s["Sub"]))
    for name, label, key in [("pick4", "PICK 4 (R9-R12)", "pick4"),
                              ("pick5", "PICK 5 (R8-R12)", "pick5"),
                              ("pick6", "PICK 6 (R7-R12)", "pick6")]:
        if key not in strategies:
            continue
        strat = strategies[key]
        story.append(Paragraph(f"<b>{label}</b>", s["Tight"]))
        rows = [["Race", "Horses (in order of model rank)", "Count"]]
        for race_num, hs in strat["legs"]:
            rows.append([f"R{race_num}", " / ".join(hs), str(len(hs))])
        rows.append(["TOTAL COMBOS", "", str(strat["combos"])])
        cost_lines = []
        for k, v in strat.items():
            if k.startswith("cost_"):
                cost_lines.append(f"{k.replace('cost_', '$').replace('c', '¢')} = ${v:.2f}")
        if cost_lines:
            rows.append(["Cost options", " | ".join(cost_lines), ""])
        tt = Table(rows, colWidths=[0.6*inch, 5.2*inch, 0.7*inch])
        tt.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#16213e")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
            ("BACKGROUND", (0, -2), (-1, -1), colors.HexColor("#fff8dc")),
        ]))
        story.append(tt)
        story.append(Spacer(1, 0.08 * inch))

    story.append(PageBreak())

    # Per-race detail
    for r in races:
        if not r["horses"]:
            continue
        story.append(Paragraph(
            f"<b>RACE {r['race']}</b> — {r.get('post_time','?')} ET  "
            f"<font size=9>· {r.get('distance','?')} {r.get('surface','?')} · "
            f"{r.get('purse','?')} · pace={r.get('pace_shape','?')} "
            f"({r.get('n_pacesetters',0)} pacesetters) · "
            f"confidence={r.get('single_confidence','?')}</font>",
            s["Sub"]))

        if r.get("power_picks"):
            story.append(Paragraph(
                f"<font size=8 color=#666>DRF Power Picks: "
                f"{', '.join(r['power_picks'])}</font>", s["Tight"]))

        rows = [["Pos", "PP", "Horse", "ML", "TFUS-E", "TFUS-L", "Style", "P×", "Score", "★"]]
        for i, h in enumerate(r["horses"][:8], 1):
            rows.append([
                str(i),
                str(h["post"]),
                h["name"],
                h["ml"] or "—",
                str(h["tfus_early"]) if h["tfus_early"] else "—",
                str(h["tfus_late"]) if h["tfus_late"] else "—",
                h["style"],
                f"{h['pace_factor']:.2f}",
                f"{h['score']:.1f}",
                "★" if h.get("power_pick") else "",
            ])
        tt = Table(rows, colWidths=[0.3*inch, 0.3*inch, 1.6*inch, 0.55*inch,
                                     0.55*inch, 0.55*inch, 0.7*inch,
                                     0.4*inch, 0.55*inch, 0.3*inch])
        tt.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1a2e")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1),
             [colors.white, colors.HexColor("#f5f5f5")]),
            ("BACKGROUND", (0, 1), (-1, 1), colors.HexColor("#d4edda")),  # winner row
        ]))
        story.append(tt)
        story.append(Spacer(1, 0.08 * inch))

    doc.build(story)
    print(f"[OK] Built {out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-pdf", action="store_true")
    args = ap.parse_args()

    if not TFUS_PDF.exists():
        print(f"[ERR] TFUS PDF not found: {TFUS_PDF}")
        sys.exit(1)

    print(f"Parsing {TFUS_PDF.name}...")
    races = parse_tfus_pdf(TFUS_PDF)
    print(f"  Found {len(races)} races")

    # Apply Derby R12 active-field filter from derby_2026_pps.json
    # The TFUS PDF includes ALL entrants (incl. AE not drawn in + scratched);
    # we only want horses with drew_in=true AND not scratched.
    derby_active_names = None
    if DERBY_PPS.exists():
        with open(DERBY_PPS) as f:
            d = json.load(f)
        derby_active_names = {
            e["name"].lower().strip()
            for e in d.get("entries", [])
            if e.get("drew_in") and not e.get("scratched")
        }

    for r in races:
        if r["race"] == 12 and derby_active_names is not None:
            before = len(r["horses"])
            r["horses"] = [h for h in r["horses"]
                           if h["name"].lower().strip() in derby_active_names]
            removed = before - len(r["horses"])
            if removed:
                print(f"  R12: filtered {removed} non-active horse(s) "
                      f"(scratches + un-drawn AEs)")
        rank_race(r)

    # Save JSON
    out_json = DATA / "full_card_picks.json"
    with open(out_json, "w") as f:
        json.dump({"races": races}, f, indent=2)
    print(f"[OK] Saved {out_json.name}")

    # Build ticket strategy
    strategies = build_ticket_strategy(races)

    # Print summary
    print("\n" + "=" * 80)
    print("  CHURCHILL DOWNS — FULL CARD AT A GLANCE")
    print("=" * 80)
    print(f"  {'Race':<5} {'Horses':<7} {'Pace':<8} {'Conf':<8} {'#1 (gap)':<35} DRF Power")
    print(f"  {'-'*5} {'-'*7} {'-'*8} {'-'*8} {'-'*35} {'-'*30}")
    for r in races:
        if not r["horses"]:
            continue
        gap = r.get("gap_1_2", 0)
        powr = ", ".join(r.get("power_picks", [])[:2]) or "—"
        print(f"  R{r['race']:<4} {len(r['horses']):<7} "
              f"{r.get('pace_shape','?'):<8} "
              f"{r.get('single_confidence','?'):<8} "
              f"{r['horses'][0]['name'] + f' ({gap:.1f})':<35} "
              f"{powr}")

    print("\n  SINGLING CANDIDATES (high model confidence):")
    for r in races:
        if r.get("single_confidence") == "STRONG" and r["horses"]:
            print(f"    R{r['race']:<2} SINGLE: {r['horses'][0]['name']} "
                  f"(gap {r['gap_1_2']:.1f})")

    print("\n  PICK 4/5/6 STRATEGIES:")
    for name, label in [("pick4", "Pick 4 (R9-R12)"),
                         ("pick5", "Pick 5 (R8-R12)"),
                         ("pick6", "Pick 6 (R7-R12)")]:
        if name not in strategies:
            continue
        strat = strategies[name]
        print(f"\n  {label}:  {strat['combos']} combos")
        for race_num, hs in strat["legs"]:
            tag = "SINGLE" if len(hs) == 1 else f"{len(hs)} horses"
            print(f"    R{race_num}: [{tag}]  {' / '.join(hs)}")

    if not args.no_pdf:
        out_pdf = DATA / "full_card_pick456_strategy.pdf"
        write_full_card_pdf(races, strategies, out_pdf)


if __name__ == "__main__":
    main()
