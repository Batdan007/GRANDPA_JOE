"""
GRANDPA_JOE CLI Entry Point

Usage:
    python -m grandpa_joe                    # Interactive mode
    python -m grandpa_joe stats              # Show brain stats
    python -m grandpa_joe handicap SAR 5     # Handicap race 5 at Saratoga
    python -m grandpa_joe ingest data.csv    # Ingest CSV data
    python -m grandpa_joe ingest-xml chart.xml  # Ingest Equibase XML chart
    python -m grandpa_joe ingest-dir ./data  # Ingest all CSV/XML/ZIP in directory
    python -m grandpa_joe crawl              # Crawl all sites for today
    python -m grandpa_joe crawl --site twinspires --date 2026-05-02  # Derby day
    python -m grandpa_joe crawl --tracks CD,KEE --date 2026-04-16    # Specific tracks
    python -m grandpa_joe fetch-data research   # Free Equibase 2023 dataset info
    python -m grandpa_joe fetch-data chart SAR 2024-08-01  # Download chart
    python -m grandpa_joe fetch-data list     # List downloaded files
    python -m grandpa_joe backfill           # Backfill computed fields
    python -m grandpa_joe train              # Train/retrain model
    python -m grandpa_joe --server           # Start API server
    python -m grandpa_joe chat               # Chat with Grandpa Joe
"""

import argparse
import sys
from typing import Optional


def show_stats(brain):
    """Display brain statistics."""
    from rich.console import Console
    from rich.table import Table

    console = Console()
    stats = brain.get_memory_stats()

    table = Table(title="Grandpa Joe's Racing Brain", show_header=True)
    table.add_column("Category", style="cyan")
    table.add_column("Count", style="green", justify="right")

    for key, value in stats.items():
        if key in ("net_pnl", "bet_win_rate"):
            continue
        table.add_row(key.replace("_", " ").title(), str(value))

    table.add_section()
    pnl = stats.get("net_pnl", 0)
    pnl_style = "green" if pnl >= 0 else "red"
    table.add_row("Net P&L", f"[{pnl_style}]${pnl:,.2f}[/{pnl_style}]")
    table.add_row("Win Rate", f"{stats.get('bet_win_rate', 0):.1f}%")

    console.print(table)


def run_ingest(brain, filepath: str):
    """Ingest a CSV file."""
    from grandpa_joe.brain.ingestion import ingest_csv
    result = ingest_csv(brain, filepath)
    print(f"Ingested: {result}")


def run_handicap(brain, track_code: str, race_number: int):
    """Run handicapping model on a race."""
    from rich.console import Console
    console = Console()

    # Find the race
    conn = brain._connect()
    try:
        race = conn.execute(
            "SELECT r.id, r.race_date, r.surface, r.distance_furlongs, "
            "r.track_condition, t.code, t.name "
            "FROM races r JOIN tracks t ON r.track_id = t.id "
            "WHERE t.code = ? AND r.race_number = ? "
            "ORDER BY r.race_date DESC LIMIT 1",
            (track_code.upper(), race_number)
        ).fetchone()
    finally:
        conn.close()

    if not race:
        console.print(f"[red]No race found: {track_code} Race {race_number}[/red]")
        return

    try:
        from grandpa_joe.models.handicapper import GrandpaJoeHandicapper
        from grandpa_joe.config import get_config
        handicapper = GrandpaJoeHandicapper(brain, get_config().model)
        rankings = handicapper.predict(race["id"])

        from rich.table import Table
        table = Table(title=f"{race['name'] or race['code']} Race {race_number} - {race['race_date']}")
        table.add_column("Rank", style="bold")
        table.add_column("Horse", style="cyan")
        table.add_column("Win%", justify="right")
        table.add_column("Confidence", justify="right")

        for r in rankings:
            table.add_row(
                str(r["rank"]),
                r["horse_name"],
                f"{r['win_probability']*100:.1f}%",
                f"{r['confidence']*100:.0f}%"
            )
        console.print(table)
    except ImportError as e:
        console.print(f"[yellow]ML dependencies not installed: {e}[/yellow]")
        console.print("Install with: pip install xgboost pandas")


def run_ingest_xml(brain, filepath: str):
    """Ingest XML chart file(s)."""
    from rich.console import Console
    console = Console()
    from pathlib import Path

    path = Path(filepath)
    if path.is_dir():
        from grandpa_joe.brain.equibase_xml import ingest_xml_directory
        result = ingest_xml_directory(brain, filepath)
        console.print(f"[green]Ingested XML directory:[/green]")
    else:
        from grandpa_joe.brain.equibase_xml import ingest_xml
        result = ingest_xml(brain, filepath)
        console.print(f"[green]Ingested XML file:[/green]")

    for k, v in result.items():
        console.print(f"  {k}: {v}")


def run_ingest_dir(brain, directory: str):
    """Ingest all CSV/XML/ZIP files from a directory."""
    from rich.console import Console
    console = Console()
    from grandpa_joe.brain.equibase_fetch import ingest_directory

    console.print(f"[cyan]Scanning {directory} for CSV, XML, and ZIP files...[/cyan]")
    result = ingest_directory(brain, directory)
    console.print(f"[green]Directory ingestion complete:[/green]")
    for k, v in result.items():
        console.print(f"  {k}: {v}")

    # Auto-backfill days_since_prev_race
    from grandpa_joe.brain.equibase_fetch import compute_days_since_previous
    updated = compute_days_since_previous(brain)
    if updated:
        console.print(f"  [dim]Backfilled days_since_prev_race: {updated} records[/dim]")


def run_fetch_data(brain, args: list):
    """Download data from Equibase."""
    from rich.console import Console
    console = Console()
    from grandpa_joe.brain.equibase_fetch import EquibaseFetcher
    from grandpa_joe.config import get_config

    config = get_config()
    fetcher = EquibaseFetcher(api_key=config.api_keys.equibase_api_key)

    if not args or args[0] == "research":
        # Download/show instructions for free research dataset
        fetcher.download_research_dataset()
    elif args[0] == "chart" and len(args) >= 3:
        # Download a specific chart: fetch-data chart SAR 2024-08-01 [csv|xml]
        track = args[1]
        date = args[2]
        fmt = args[3] if len(args) > 3 else "csv"
        path = fetcher.download_chart(track, date, fmt)
        if path:
            console.print(f"[green]Downloaded: {path}[/green]")

            # Auto-ingest
            console.print("[cyan]Auto-ingesting...[/cyan]")
            if fmt == "xml":
                from grandpa_joe.brain.equibase_xml import ingest_xml
                result = ingest_xml(brain, path)
            else:
                from grandpa_joe.brain.ingestion import ingest_csv
                result = ingest_csv(brain, path)
            for k, v in result.items():
                console.print(f"  {k}: {v}")
        else:
            console.print("[red]Download failed. Check your EQUIBASE_API_KEY.[/red]")
    elif args[0] == "status":
        status = fetcher.get_status()
        for k, v in status.items():
            console.print(f"  {k}: {v}")
    elif args[0] == "list":
        files = fetcher.list_local_files()
        if files:
            from rich.table import Table
            table = Table(title="Local Data Files")
            table.add_column("File", style="cyan")
            table.add_column("Format")
            table.add_column("Size (MB)", justify="right")
            for f in files:
                table.add_row(f["name"], f["format"], str(f["size_mb"]))
            console.print(table)
        else:
            console.print("[yellow]No data files found.[/yellow]")
    else:
        console.print("""[bold]Usage:[/bold]
  grandpa_joe fetch-data research              # Free 2023 research dataset instructions
  grandpa_joe fetch-data chart SAR 2024-08-01  # Download chart (needs API key)
  grandpa_joe fetch-data chart SAR 2024-08-01 xml  # Download XML format
  grandpa_joe fetch-data status                # Show fetcher status
  grandpa_joe fetch-data list                  # List downloaded files""")


def run_backfill(brain):
    """Backfill computed fields."""
    from rich.console import Console
    console = Console()
    from grandpa_joe.brain.equibase_fetch import compute_days_since_previous

    console.print("[cyan]Backfilling days_since_prev_race...[/cyan]")
    updated = compute_days_since_previous(brain)
    console.print(f"[green]Updated {updated} records[/green]")


def run_train(brain, before_date: Optional[str] = None):
    """Train the handicapping model."""
    from rich.console import Console
    console = Console()
    try:
        from grandpa_joe.models.trainer import train_model
        from grandpa_joe.config import get_config
        metrics = train_model(brain, get_config().model, before_date=before_date)
        console.print(f"[green]Model trained![/green]")
        for k, v in metrics.items():
            console.print(f"  {k}: {v}")
    except ImportError as e:
        console.print(f"[yellow]ML dependencies not installed: {e}[/yellow]")


def run_crawl_cmd(brain, args):
    """Run the web crawler and ingest results."""
    from rich.console import Console
    from datetime import date as Date
    console = Console()

    try:
        from grandpa_joe.crawlers.runner import run_crawl
    except ImportError as e:
        console.print(f"[yellow]Crawler deps not installed: {e}[/yellow]")
        console.print("Install with: pip install 'grandpa-joe[crawl]'")
        return

    site_names = None
    if args.site and args.site.lower() != "all":
        site_names = [s.strip() for s in args.site.split(",") if s.strip()]
    track_codes = None
    if args.tracks:
        track_codes = [t.strip().upper() for t in args.tracks.split(",") if t.strip()]
    target_date = args.date
    if target_date in (None, "today"):
        target_date = str(Date.today())

    console.print(f"[cyan]Crawling {site_names or 'all sites'} for {target_date}...[/cyan]")
    summary = run_crawl(
        brain,
        site_names=site_names,
        track_codes=track_codes,
        target_date=target_date,
        use_nexus=args.nexus,
    )

    console.print(f"[green]Crawl complete:[/green] "
                  f"{summary.races_crawled} races, "
                  f"{summary.entries_crawled} entries, "
                  f"{summary.results_ingested} results ingested")
    if summary.errors:
        console.print(f"[yellow]{len(summary.errors)} errors:[/yellow]")
        for err in summary.errors[:10]:
            console.print(f"  - {err}")


def run_server():
    """Start the FastAPI server."""
    try:
        import uvicorn
        from grandpa_joe.config import get_config
        config = get_config()
        uvicorn.run(
            "grandpa_joe.api.server:app",
            host=config.server.host,
            port=config.server.port,
            reload=config.server.debug,
        )
    except ImportError:
        print("FastAPI/uvicorn not installed. Run: pip install fastapi uvicorn")
        sys.exit(1)


def interactive_mode(brain):
    """Simple interactive chat mode."""
    from rich.console import Console
    console = Console()

    console.print("[bold green]" + r"""
   ____                     _                   _
  / ___|_ __ __ _ _ __   __| |_ __   __ _      | | ___   ___
 | |  _| '__/ _` | '_ \ / _` | '_ \ / _` |  _  | |/ _ \ / _ \
 | |_| | | | (_| | | | | (_| | |_) | (_| | | |_| | (_) |  __/
  \____|_|  \__,_|_| |_|\__,_| .__/ \__,_|  \___/ \___/ \___|
                              |_|
    """ + "[/bold green]")
    console.print("[dim]The wise old handicapper. Type 'quit' to exit.[/dim]\n")

    stats = brain.get_memory_stats()
    console.print(f"[dim]Brain: {stats['horses']} horses, {stats['races']} races, "
                  f"{stats['past_performances']} past performances[/dim]\n")

    while True:
        try:
            user_input = console.input("[bold cyan]You:[/bold cyan] ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not user_input or user_input.lower() in ("quit", "exit", "/exit"):
            console.print("\n[green]Grandpa Joe:[/green] See ya at the track, kid.")
            break

        if user_input.lower() == "stats":
            show_stats(brain)
            continue

        # For now, basic responses until personality module is built
        console.print(f"\n[green]Grandpa Joe:[/green] I hear ya. Once we get my model "
                      f"trained up, I'll have picks for ya. For now, try 'stats' or "
                      f"run me with 'ingest' to feed me some data.\n")


def main():
    parser = argparse.ArgumentParser(
        description="Grandpa Joe - Horse Racing Handicapping Assistant"
    )
    parser.add_argument("command", nargs="?", default="interactive",
                        choices=["interactive", "stats", "handicap", "ingest",
                                 "ingest-xml", "ingest-dir", "fetch-data",
                                 "backfill", "train", "chat", "crawl"],
                        help="Command to run")
    parser.add_argument("args", nargs="*", help="Command arguments")
    parser.add_argument("--server", action="store_true", help="Start API server")
    parser.add_argument("--version", action="store_true", help="Show version")
    parser.add_argument("--site", default=None,
                        help="Crawler site filter: all|twinspires|equibase|drf (comma-separated)")
    parser.add_argument("--tracks", default=None,
                        help="Track codes for crawl (comma-separated, e.g. CD,KEE,GP)")
    parser.add_argument("--date", default=None,
                        help="Target date YYYY-MM-DD (default: today). 'today' is allowed.")
    parser.add_argument("--before-date", default=None,
                        help="For 'train': cutoff YYYY-MM-DD; only train on races before this.")
    parser.add_argument("--nexus", action="store_true",
                        help="Forward crawled data to ALFRED/CORTEX via NEXUS (off by default)")

    args = parser.parse_args()

    if args.version:
        from grandpa_joe import __version__
        print(f"Grandpa Joe v{__version__}")
        return

    if args.server:
        run_server()
        return

    # Initialize brain
    from grandpa_joe.path_manager import PathManager
    PathManager.ensure_all_paths()

    from grandpa_joe.brain import RacingBrain
    brain = RacingBrain()

    if args.command == "stats":
        show_stats(brain)
    elif args.command == "handicap":
        if len(args.args) < 2:
            print("Usage: grandpa_joe handicap <TRACK_CODE> <RACE_NUMBER>")
            sys.exit(1)
        run_handicap(brain, args.args[0], int(args.args[1]))
    elif args.command == "ingest":
        if not args.args:
            print("Usage: grandpa_joe ingest <CSV_FILE>")
            sys.exit(1)
        run_ingest(brain, args.args[0])
    elif args.command == "ingest-xml":
        if not args.args:
            print("Usage: grandpa_joe ingest-xml <XML_FILE_OR_DIR>")
            sys.exit(1)
        run_ingest_xml(brain, args.args[0])
    elif args.command == "ingest-dir":
        if not args.args:
            print("Usage: grandpa_joe ingest-dir <DIRECTORY>")
            sys.exit(1)
        run_ingest_dir(brain, args.args[0])
    elif args.command == "fetch-data":
        run_fetch_data(brain, args.args)
    elif args.command == "backfill":
        run_backfill(brain)
    elif args.command == "train":
        run_train(brain, before_date=args.before_date)
    elif args.command == "crawl":
        run_crawl_cmd(brain, args)
    elif args.command in ("interactive", "chat"):
        interactive_mode(brain)


if __name__ == "__main__":
    main()
