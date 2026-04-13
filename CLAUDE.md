# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start

```bash
# Install core (Python 3.11 or 3.12 ONLY)
pip install -e .

# Install with ML support
pip install -e ".[ml]"

# Install with API server
pip install -e ".[api]"

# Install everything
pip install -e ".[full]"

# Run
python -m grandpa_joe              # Interactive mode
python -m grandpa_joe stats        # Brain statistics
python -m grandpa_joe handicap SAR 5  # Handicap race 5 at Saratoga
python -m grandpa_joe ingest data.csv # Ingest CSV data
python -m grandpa_joe train        # Train/retrain model
python -m grandpa_joe --server     # Start API server on port 8100
python -m grandpa_joe chat         # Chat with Grandpa Joe
```

## Running Tests

```bash
python -m pytest tests/
python tests/test_brain.py         # Run a single test file directly
python tests/test_kelly.py
python tests/test_ethics.py
```

Tests use `tempfile.TemporaryDirectory()` for isolated SQLite instances — no cleanup needed.

## Architecture

```
User/Mobile App
     |
     v
FastAPI (port 8100)    --NEXUS-->  ALFRED (port 8000)
     |                  REST API    (patent tech stays here)
     v
RacingBrain (SQLite + WAL + in-memory caches)
     |
     v
XGBoost Handicapper (20-feature model, morning line fallback)
     |
     v
Kelly Criterion Bet Sizing (quarter-Kelly default)
     |
     v
Responsible Gambling Guard (checked before EVERY bet)
```

### Data Flow

1. **Ingestion**: CSV -> `brain.ingestion.ingest_csv()` -> SQLite (12 tables)
2. **Feature Engineering**: `models.features.build_features_for_race()` computes 20 features per entry using `brain.queries` helper functions
3. **Prediction**: `models.handicapper.GrandpaJoeHandicapper.predict()` -> XGBoost or morning-line fallback
4. **Bet Sizing**: `models.kelly.suggest_bets()` -> fractional Kelly with min $2 / max 10% bankroll caps
5. **Ethics Gate**: `ethics.responsible_gambling.ResponsibleGamblingGuard.check_bet()` -> HARD block or SOFT warning before any bet is recorded or suggested

### Key Patterns

**PathManager** — all file paths must go through this:
```python
from grandpa_joe.path_manager import PathManager
db_path = PathManager.BRAIN_DB  # CORRECT
# db_path = "C:/path/..."       # WRONG — never hardcode paths
```
Override root with `GRANDPA_JOE_HOME` env var. Platform-specific defaults: Windows=`C:/Drive`, macOS=`~/Library/Application Support/GrandpaJoe`, Linux=`~/.grandpa_joe`.

**Graceful Degradation** — ML, API, and NEXUS are all optional. Core brain always works:
```python
try:
    import xgboost
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False
```
Every module that uses optional deps (xgboost, pandas, fastapi, httpx) follows this pattern.

**Config Singleton** — `grandpa_joe.config.get_config()` returns a `GrandpaJoeConfig` dataclass. Loads from `.env` file (via python-dotenv) then env vars then `config.json`. Use `reload_config()` to force refresh.

**RacingBrain Caches** — `horse_cache`, `jockey_cache`, `trainer_cache`, `track_cache` are in-memory dicts (name/code -> id) loaded at init. The `get_or_create_*` methods use INSERT OR IGNORE + cache.

**API State** — FastAPI app stores shared instances on `app.state`: `brain`, `config`, `guard`, `handicapper`, `nexus`. Routes access these via `request.app.state`.

## IP Protection Rules

**DO NOT** include any of these in this repo:
- CORTEX (5-layer forgetting) source code
- ULTRATHUNK (generative compression) source code
- Guardian (behavioral watermarking) source code
- NEXUSRouter or IntentTranslator internals

These are patent-pending and live in the ALFRED repo only.
GRANDPA_JOE consumes them via NEXUS REST API (`nexus.client.NexusClient`), which posts signed JSON messages to ALFRED's `/v1/nexus/message` endpoint.

## Database Schema (12 tables)

tracks, horses, jockeys, trainers, races, entries, results,
past_performances, predictions, bets, handicapping_patterns,
gambling_session_log

Schema defined in `grandpa_joe/brain/schema.py`. SQLite with WAL mode and foreign keys enabled.

## Environment Variables

```bash
GRANDPA_JOE_HOME="/custom/path"     # Override root directory
ALFRED_URL="http://127.0.0.1:8000"  # NEXUS bridge to ALFRED
NEXUS_SECRET="shared-secret"        # NEXUS HMAC-SHA256 auth
EQUIBASE_API_KEY="..."              # Racing data provider
DRF_API_KEY="..."                   # Daily Racing Form
DAILY_LOSS_LIMIT="100.00"           # Responsible gambling
MAX_SINGLE_BET="50.00"              # Max single bet
SESSION_TIME_LIMIT="120"            # Session limit in minutes
GRANDPA_JOE_PORT="8100"             # API server port
GRANDPA_JOE_DEBUG="false"           # Debug mode
```

## Personality

Grandpa Joe: Wise old handicapper. Folksy, knows the track.
"Now this number 5 horse, let me tell ya..."
Not sycophantic. Honest about bad bets. Cares about responsible gambling.
Ethics guard messages use this voice (e.g., "Son, I can't let you place this bet").

## Equibase Free Data Set (2023 Calendar Year)

Complete Past Performances and Result Charts for 2023, stored in `DATA SETS/`.

### Past Performance Files

- **Source archive**: `2023 PPs.zip`
- **Naming convention**: `SIMDyyyymmddTRK_CTR.xml`
  - `SIMD` = file type
  - `yyyy` = year, `mm` = month, `dd` = day
  - `TRK` = 2-or-3 character track code
  - `CTR` = 2-or-3 character country code
- **Schema**: http://ifd.equibase.com/schema/simulcast.xsd

### Result Charts

- **Source archive**: `2023 Result Charts.zip`
- **Naming convention**: `TRKyyyymmddtch.xml`
  - `TRK` = 2-or-3 character track code
  - `yyyy` = year, `mm` = month, `dd` = day
  - `tch` = file type
- **Schema**: https://info.trackmaster.com/xmlSchema/tchSchema.xsd

## Author

Daniel J Rita (BATDAN) | GxEum Technologies / CAMDAN Enterprizes
