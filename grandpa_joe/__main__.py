"""
GRANDPA_JOE CLI Entry Point

Usage:
    python -m grandpa_joe                    # Interactive mode
    python -m grandpa_joe stats              # Show brain stats
    python -m grandpa_joe handicap SAR 5     # Handicap race 5 at Saratoga
    python -m grandpa_joe ingest data.csv    # Ingest CSV data
    python -m grandpa_joe train              # Train/retrain model
    python -m grandpa_joe --server           # Start API server
    python -m grandpa_joe chat               # Chat with Grandpa Joe
"""

import argparse
import sys


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


def run_train(brain):
    """Train the handicapping model."""
    from rich.console import Console
    console = Console()
    try:
        from grandpa_joe.models.trainer import train_model
        from grandpa_joe.config import get_config
        metrics = train_model(brain, get_config().model)
        console.print(f"[green]Model trained![/green]")
        for k, v in metrics.items():
            console.print(f"  {k}: {v}")
    except ImportError as e:
        console.print(f"[yellow]ML dependencies not installed: {e}[/yellow]")


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
                                 "train", "chat"],
                        help="Command to run")
    parser.add_argument("args", nargs="*", help="Command arguments")
    parser.add_argument("--server", action="store_true", help="Start API server")
    parser.add_argument("--version", action="store_true", help="Show version")

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
    elif args.command == "train":
        run_train(brain)
    elif args.command in ("interactive", "chat"):
        interactive_mode(brain)


if __name__ == "__main__":
    main()
