"""Bundle the Derby Day animations + betting sheets into a self-contained zip
that DJR can share with friends. Friends just unzip and double-click START_HERE.html.

Output: data/GRANDPA_JOE_DERBY_2026_SHARE.zip
"""
from __future__ import annotations
import shutil
import zipfile
from pathlib import Path
from datetime import datetime

REPO = Path(__file__).resolve().parent.parent
BUNDLE = REPO / "bundle" / "race_3d"
DATA = REPO / "data"
OUT = DATA / "GRANDPA_JOE_DERBY_2026_SHARE.zip"
STAGE = DATA / "_share_staging"


START_HERE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Grandpa Joe — 2026 Kentucky Derby Day</title>
<style>
body{font-family:'Segoe UI',system-ui,sans-serif;background:#0f0f23;color:#f0f0f0;
  padding:30px;max-width:980px;margin:auto;line-height:1.5}
h1{color:#ffd700;border-bottom:3px solid #ffd700;padding-bottom:10px;font-size:32px}
h2{color:#ffd700;margin-top:35px;font-size:22px;
  border-left:4px solid #ffd700;padding-left:12px}
.card{background:#1a1a2e;border:1px solid #333;border-radius:8px;padding:20px;
  margin:14px 0}
.big-link{display:block;background:#ffd700;color:#0f0f23;padding:18px 22px;
  border-radius:8px;font-weight:bold;font-size:18px;text-decoration:none;
  text-align:center;margin:14px 0}
.big-link:hover{background:#ffea4d}
.sub-link{display:inline-block;background:#444;color:#ffd700;padding:10px 16px;
  border-radius:6px;text-decoration:none;margin:6px 8px 6px 0}
.sub-link:hover{background:#555}
.note{color:#aaa;font-size:14px;margin:8px 0}
ul{padding-left:24px}
li{margin:8px 0}
.tag{display:inline-block;background:#28a745;color:#fff;padding:2px 8px;
  border-radius:3px;font-size:12px;font-weight:bold;margin-left:6px}
.med{background:#ffc107;color:#000}
.wide{background:#dc3545;color:#fff}
code{background:#222;padding:2px 6px;border-radius:3px;font-family:Consolas,monospace}
.foot{color:#888;font-size:12px;margin-top:50px;border-top:1px solid #333;padding-top:14px}
</style>
</head>
<body>

<h1>Grandpa Joe — 2026 Kentucky Derby Day</h1>
<p class="note">Saturday May 2 2026 · Churchill Downs · 14-race card<br>
Pace-adjusted picks built from DRF/TimeformUS data on May 1.</p>

<div class="card">
<h2 style="margin-top:0">Watch the races (animated playback)</h2>
<p>Hit ▶ Play on any animation. Each horse moves around the oval in real time;
the running-order panel updates as the race unfolds, then snaps to ORDER OF FINISH
at the wire.</p>

<a class="big-link" href="bundle/race_3d/full_card_index.html">▶ Open the Full Card Dashboard (all 14 races)</a>

<a class="sub-link" href="bundle/race_3d/derby_2026_animated.html">152nd Kentucky Derby (R12) — full TFUS-adjusted playback</a>
</div>

<div class="card">
<h2 style="margin-top:0">Printable betting sheets</h2>
<a class="sub-link" href="docs/derby_2026_betting_sheet.pdf">Derby (R12) printable betting sheet</a>
<a class="sub-link" href="docs/full_card_pick456_strategy.pdf">Full-card Pick 4/5/6 strategy</a>
<a class="sub-link" href="docs/all_races_order_of_finish.pdf">Order-of-finish summary</a>
<p class="note">These are letter-size PDFs designed for single-sided printing.</p>
</div>

<div class="card">
<h2 style="margin-top:0">Singling candidates (high-confidence races)</h2>
<p>Races where the model's gap between #1 and #2 is so wide that singling that
leg in a Pick 4/5/6 is statistically defensible:</p>
<ul>
<li><b>R10 — Crazy Mason</b><span class="tag">SINGLE</span> · gap 20.8 · model loves him over the chalk</li>
<li><b>R12 — Renegade</b><span class="tag">SINGLE</span> · gap 31.3 · highest TFUS-Late (116) in the field</li>
<li><b>R13 — Buetane</b><span class="tag">SINGLE</span> · gap 16.9 · late-card stand-out</li>
</ul>
<p class="note">Singling R10 alone cuts a Pick 6 from $300+ down to $32 at 20¢ base.</p>
</div>

<div class="card">
<h2 style="margin-top:0">152nd Kentucky Derby — pace-adjusted top 5</h2>
<ul>
<li><b>1. PP6 Commandment</b> (6-1) — pace-adjusted #1 chalk · Cox + Saez</li>
<li><b>2. PP1 Renegade</b> (4-1) — TFUS-Late 116 elite, but rail post is the trap</li>
<li><b>3. PP9 The Puma</b> (10-1) — late-kick stalker · Castellano</li>
<li><b>4. PP8 So Happy</b> (15-1) — pace-trap haircut, slight overlay</li>
<li><b>5. PP5 Right to Party</b> (30-1) — top price play · deep closer in melt</li>
</ul>
<p class="note">Scratched: PP13 Silent Tactic, PP20 Fulleffort. AEs drew in: PP21 Great White, PP22 Ocelli.<br>
Pace shape: <b>FAST</b> (6 pacesetters with TFUS-E ≥ 98) — closers and late-kick stalkers favored.</p>
</div>

<div class="card">
<h2 style="margin-top:0">How to use these files</h2>
<ol>
<li>Extract this zip anywhere on your computer (Desktop is easiest).</li>
<li>Double-click <code>START_HERE.html</code> — opens this page in your browser.</li>
<li>Click <b>Open the Full Card Dashboard</b> to pick any race and watch it play out.</li>
<li>The PDFs in <code>docs/</code> are the printable betting sheets for the track.</li>
</ol>
<p class="note"><b>Internet required:</b> the animations load the Plotly charting library
from a public CDN, so you need to be online when you open them. Other than that
everything runs in your browser — no install, no signup, nothing to set up.</p>
</div>

<p class="foot">
Built by Grandpa Joe (DJR / CAMDAN Enterprises) · GxEum Technologies<br>
Generated MAKEFILE_TIMESTAMP. Picks are educational projections — bet responsibly.
</p>
</body></html>
"""


README_TXT = """GRANDPA JOE - 2026 KENTUCKY DERBY DAY SHARE BUNDLE
==================================================

Open START_HERE.html (double-click) for the dashboard.

Contents:
  START_HERE.html                 main landing page
  bundle/race_3d/                 14 animated race playbacks + index
  docs/                           printable betting sheets (PDF)
  README.txt                      this file

Need internet to play the animations (Plotly library loads from CDN).
Everything else works offline.

Built MAKEFILE_TIMESTAMP by Grandpa Joe (DJR / CAMDAN).
"""


def main():
    # Clean staging
    if STAGE.exists():
        shutil.rmtree(STAGE)
    STAGE.mkdir(parents=True)

    timestamp = datetime.now().strftime("%a %b %d %Y %I:%M %p")

    # Write START_HERE
    landing = START_HERE.replace("MAKEFILE_TIMESTAMP", timestamp)
    (STAGE / "START_HERE.html").write_text(landing, encoding="utf-8")
    (STAGE / "README.txt").write_text(
        README_TXT.replace("MAKEFILE_TIMESTAMP", timestamp), encoding="utf-8"
    )

    # Copy bundle/race_3d/ — only the animations + indices
    bundle_target = STAGE / "bundle" / "race_3d"
    bundle_target.mkdir(parents=True)
    keep = [
        "full_card_index.html",
        "index.html",
        "derby_2026_animated.html",
    ]
    keep += [f"R{n}_full_card_animated.html" for n in range(1, 15)]
    # Plus per-race static plots if they exist
    keep += [f"CD_R{n}_2026-05-02.html" for n in [7, 8, 9, 10, 11, 12]]
    for fn in keep:
        src = BUNDLE / fn
        if src.exists():
            shutil.copy2(src, bundle_target / fn)
    print(f"  Copied {len(list(bundle_target.iterdir()))} bundle files")

    # Copy PDFs to docs/
    docs_target = STAGE / "docs"
    docs_target.mkdir()
    for src_name in [
        "derby_2026_betting_sheet.pdf",
        "full_card_pick456_strategy.pdf",
        "all_races_order_of_finish.pdf",
    ]:
        src = DATA / src_name
        if src.exists():
            shutil.copy2(src, docs_target / src_name)
            print(f"  Copied {src_name}")

    # Build the zip
    if OUT.exists():
        OUT.unlink()
    with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for path in STAGE.rglob("*"):
            if path.is_file():
                arcname = path.relative_to(STAGE)
                zf.write(path, arcname)

    size_mb = OUT.stat().st_size / (1024 * 1024)
    file_count = sum(1 for _ in zipfile.ZipFile(OUT).infolist())

    # Cleanup staging
    shutil.rmtree(STAGE)

    print()
    print("=" * 70)
    print(f"  SHARE ZIP READY")
    print("=" * 70)
    print(f"  Path:   {OUT}")
    print(f"  Size:   {size_mb:.1f} MB")
    print(f"  Files:  {file_count}")
    print()
    print("  Send to friends. They just need to:")
    print("    1. Unzip anywhere")
    print("    2. Double-click START_HERE.html")
    print("    3. Be online (Plotly loads from CDN)")
    print("=" * 70)


if __name__ == "__main__":
    main()
