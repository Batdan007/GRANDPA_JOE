"""Bulk-ingest the 2023 Equibase SIMD XMLs into the brain.

Monkey-patches RacingBrain to use ONE shared SQLite connection with batched
commits — bypasses the per-write fsync that caused 38s/file on the naive path.

Progress logged to logs/bulk_ingest.log.
"""

import logging
import sqlite3
import sys
import time
import types
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from grandpa_joe.brain.racing_brain import RacingBrain
from grandpa_joe.brain.equibase_simd import ingest_simd

LOG_DIR = REPO / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "bulk_ingest.log", mode="a"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

COMMIT_EVERY_N_FILES = 25


class _SharedConn:
    """Wraps a sqlite3.Connection so close() and commit() are no-ops.

    The owning caller commits/closes via real_commit()/real_close().
    Methods called by RacingBrain see a standard Connection-like object.
    """

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def real_commit(self):
        self._conn.commit()

    def real_close(self):
        self._conn.close()

    # no-op close/commit for brain-internal use
    def close(self):
        return None

    def commit(self):
        return None

    def __getattr__(self, name):
        return getattr(self._conn, name)


def bulk_patch(brain: RacingBrain) -> "_SharedConn":
    """Replace brain._connect with a shared no-close/no-commit connection."""
    conn = sqlite3.connect(str(brain.db_path), timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-100000")
    conn.execute("PRAGMA temp_store=MEMORY")
    shared = _SharedConn(conn)
    brain._connect = lambda: shared
    return shared


def main():
    files = sorted((REPO / "DATA SETS").glob("**/SIMD*.xml"))
    dedup = {}
    for f in files:
        if f.name not in dedup or len(f.parts) > len(dedup[f.name].parts):
            dedup[f.name] = f
    files = sorted(dedup.values())
    log.info("Found %d unique SIMD XML files", len(files))

    brain = RacingBrain()
    shared = bulk_patch(brain)
    shared.execute("BEGIN")

    totals = {}
    t0 = time.time()
    failed = []

    try:
        for i, f in enumerate(files, 1):
            try:
                counts = ingest_simd(brain, str(f))
                for k, v in counts.items():
                    totals[k] = totals.get(k, 0) + v
            except Exception as e:
                log.error("FAILED %s: %s", f.name, e)
                failed.append(f.name)

            if i % COMMIT_EVERY_N_FILES == 0:
                shared.real_commit()
                shared.execute("BEGIN")

            if i % 50 == 0 or i == len(files):
                elapsed = time.time() - t0
                rate = i / elapsed if elapsed else 0
                eta_min = (len(files) - i) / rate / 60 if rate else 0
                log.info(
                    "Progress %d/%d (%.2f files/s, ETA %.1f min) totals=%s failures=%d",
                    i, len(files), rate, eta_min, totals, len(failed),
                )

        shared.real_commit()
    except Exception:
        log.exception("Aborting bulk ingest")
        try:
            shared.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        shared.real_close()

    log.info("DONE. Final totals: %s", totals)
    log.info("Failed files (%d): %s", len(failed), failed[:20])


if __name__ == "__main__":
    main()
