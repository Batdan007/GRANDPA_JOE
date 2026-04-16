"""
Daily crawl scheduler.

Two modes:
  - loop mode: runs in-process with APScheduler (for long-lived dev/service use)
  - one-shot mode: invoked by OS scheduler (Windows Task Scheduler / cron)

For Derby prep we recommend the OS-scheduler path — it's simpler and doesn't
require GRANDPA_JOE to be running 24/7.
"""

import logging
from datetime import date as Date
from typing import Optional

logger = logging.getLogger(__name__)


def run_once(brain, target_date: Optional[str] = None) -> dict:
    """Run one crawl pass. Suitable for OS cron/Task Scheduler."""
    from grandpa_joe.crawlers.runner import run_crawl
    summary = run_crawl(brain, target_date=target_date)
    return summary.model_dump()


def run_forever(brain, hour: int = 6, minute: int = 0):
    """
    Run daily at hour:minute local time using APScheduler.
    Blocks — call from a process you're willing to keep alive.
    """
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        raise RuntimeError(
            "APScheduler not installed. Run: pip install 'grandpa-joe[crawl]'"
        )

    scheduler = BlockingScheduler(timezone="US/Eastern")
    scheduler.add_job(
        run_once,
        CronTrigger(hour=hour, minute=minute),
        args=[brain],
        id="daily_crawl",
        replace_existing=True,
    )
    logger.info(f"Scheduler armed — daily crawl at {hour:02d}:{minute:02d} ET")
    scheduler.start()


def windows_task_cmd(python_exe: str = "py", date_flag: str = "today") -> str:
    """Build the schtasks CLI command to register a daily task."""
    return (
        f'schtasks /Create /SC DAILY /TN "GrandpaJoeDailyCrawl" '
        f'/TR "{python_exe} -3.11 -m grandpa_joe crawl --date {date_flag}" '
        f'/ST 06:00 /F'
    )
