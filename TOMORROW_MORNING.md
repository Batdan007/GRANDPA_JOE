# DERBY DAY MORNING — Saturday May 2 2026

Step-by-step workflow to refresh every race's order-of-finish prediction and animation
once you have all the per-race DRF PPs.

## 1. Pull latest from GitHub
```powershell
cd C:\Users\danie\GRANDPA_JOE
git pull origin main
```

## 2. Drop your fresh DRF PP PDFs into `data/`
Save each race's DRF PPs PDF to `data/` with this filename pattern:

```
data\CD-R7--05-02-2026.pdf
data\CD-R8--05-02-2026.pdf
data\CD-R9--05-02-2026.pdf
data\CD-R10--05-02-2026.pdf
data\CD-R11--05-02-2026.pdf
data\CD--05-02-2026.pdf          (Derby — already there)
```

(Optional: also R1-R6 and R13-R14 if you want full-card coverage. The
processor matches `CD-R{n}--05-02-2026.pdf` for any n.)

## 3. Update Derby scratches (if any new ones overnight)
Edit `data/derby_2026_pps.json`:
- Find the scratched horse, set `"drew_in": false` and add `"scratched": true`.
- If a new AE drew in, add it as a new entry with `"drew_in": true` and the
  assigned post (likely 23 if Robusta is next).

## 4. Run the full pipeline
```powershell
# Re-run the Derby pace-adjusted pipeline (uses the updated pps.json)
python scripts\derby_2026_picks.py
python scripts\derby_2026_pace_overlay.py
python scripts\derby_2026_dan_style_tickets.py
python scripts\derby_2026_3d.py
python scripts\derby_2026_animate.py

# Process every race PDF in data/ — ranks each, builds the all-races PDF
python scripts\process_all_races.py --pdf

# Rebuild the printable betting sheet
python scripts\build_derby_betting_pdf.py
```

## 5. Open everything
```powershell
ii data\derby_2026_betting_sheet.pdf            # printable betting sheet
ii data\all_races_order_of_finish.pdf           # full card order of finish
ii bundle\race_3d\derby_2026_animated.html      # animated Derby playback
ii bundle\race_3d\index.html                    # all-races dashboard
```

## 6. Print and head to the party
The two PDFs (`derby_2026_betting_sheet.pdf` and `all_races_order_of_finish.pdf`)
are designed for letter-size single-sided printing.

---

## What the multi-race processor does

For each race PDF in `data/CD-R*--05-02-2026.pdf`:
1. Parses the DRF PPs with PyMuPDF
2. Extracts each horse's TFUS Early/Late, post, ML, jockey
3. Filters scratched horses (cross-references `derby_2026_pps.json`)
4. Computes a pace-overlay score:
   - Base = TFUS Late × 1.0 + bonus for low Early
   - Multiplier = 1.5x for deep closers in fast-pace shape, 0.55x for high-Early in fast-pace, 1.3x for late-kick stalkers, 1.0 baseline
5. Outputs ranked top-of-finish to `data/CD_R{n}_2026-05-02_picks.json`
6. Builds combined `data/all_races_order_of_finish.pdf` with each race's top 8

## Limitations (heads up)

- The multi-race processor uses **TFUS pace fit only** — no graded-stakes
  pattern, post-bias, or trainer/jockey adjustments. It's a fast first pass,
  not a full handicap. The Derby (R12) has all those layers in
  `derby_2026_picks.py` + `derby_2026_pace_overlay.py`.
- DRF PDF parser is heuristic. If a race's PDF has unusual layout, some
  horses may be missed. Cross-check the `Top 5` printout vs. the actual entries.
- For races without TFUS pace lines (turf imports, foreign shippers), the
  parser falls back to neutral scoring.

## If a script errors

| Error | Fix |
|---|---|
| `ModuleNotFoundError: pymupdf` | `pip install pymupdf` |
| `ModuleNotFoundError: plotly` | `pip install plotly` |
| `ModuleNotFoundError: reportlab` | `pip install reportlab` |
| `[ERR] No race PDFs found` | Filename pattern wrong — must be `CD-R{n}--05-02-2026.pdf` |
| Empty top-5 for a race | DRF parser couldn't find TFUS lines — paste the PDF text and Claude can patch the regex |
| Animation file too small / no horses | Race has no TFUS data; check the PDF parses with `python -c "import fitz; print(fitz.open('data/CD-R7--05-02-2026.pdf')[0].get_text())"` |
