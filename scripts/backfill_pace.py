"""Back-fill horse_pace_history from SIMD XMLs.

Uses the same shared-connection pattern as bulk_ingest_2023 to move at
~0.3 s/file rather than ~38 s/file.

Prereq: RacingBrain migrations run (creates the horse_pace_history table).
"""

import logging
import sqlite3
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from grandpa_joe.brain.racing_brain import RacingBrain
from grandpa_joe.brain.pace_extract import extract_pace_rows

LOG_DIR = REPO / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "backfill_pace.log", mode="a"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

COMMIT_EVERY_N_FILES = 25


class _SharedConn:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def real_commit(self):
        self._conn.commit()

    def real_close(self):
        self._conn.close()

    def close(self):
        return None

    def commit(self):
        return None

    def __getattr__(self, name):
        return getattr(self._conn, name)


def bulk_patch(brain: RacingBrain) -> "_SharedConn":
    conn = sqlite3.connect(str(brain.db_path), timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-100000")
    conn.execute("PRAGMA temp_store=MEMORY")
    return _SharedConn(conn)


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

    total_rows = 0
    failed: list[str] = []
    unmapped_horses = 0
    t0 = time.time()

    insert_sql = (
        "INSERT OR IGNORE INTO horse_pace_history "
        "(horse_id, race_date, track_code, distance_furlongs, surface, "
        "call_id, call_order, position, lengths_behind, leader_time_sec, "
        "horse_time_sec, speed_figure) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    try:
        for i, f in enumerate(files, 1):
            try:
                rows = extract_pace_rows(str(f))
            except Exception as e:
                log.error("FAILED %s: %s", f.name, e)
                failed.append(f.name)
                continue

            payload = []
            for r in rows:
                horse_id = brain.horse_cache.get(r["horse_name"])
                if not horse_id:
                    unmapped_horses += 1
                    continue
                payload.append((
                    horse_id, r["race_date"], r["track_code"],
                    r["distance_furlongs"], r["surface"],
                    r["call_id"], r["call_order"],
                    r["position"], r["lengths_behind"],
                    r["leader_time_sec"], r["horse_time_sec"],
                    r["speed_figure"],
                ))
            if payload:
                shared.executemany(insert_sql, payload)
                total_rows += len(payload)

            if i % COMMIT_EVERY_N_FILES == 0:
                shared.real_commit()
                shared.execute("BEGIN")

            if i % 50 == 0 or i == len(files):
                elapsed = time.time() - t0
                rate = i / elapsed if elapsed else 0
                eta = (len(files) - i) / rate / 60 if rate else 0
                log.info(
                    "Progress %d/%d (%.2f f/s, ETA %.1f min) rows=%s unmapped_horses=%s failures=%d",
                    i, len(files), rate, eta, total_rows, unmapped_horses, len(failed),
                )
        shared.real_commit()
    except Exception:
        log.exception("Aborting backfill")
        try:
            shared.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        shared.real_close()

    log.info("DONE. Total pace rows: %s. Unmapped horses (PP horses never entered): %s. Failures: %d",
             total_rows, unmapped_horses, len(failed))


if __name__ == "__main__":
    main()
