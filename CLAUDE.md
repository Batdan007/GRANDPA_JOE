# CLAUDE.md - GRANDPA_JOE

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
python -m grandpa_joe --server     # API on port 8100
```

## Architecture

```
User/Mobile App
     │
     ▼
FastAPI (port 8100)    ──NEXUS──▶  ALFRED (port 8000)
     │                  REST API    (patent tech stays here)
     ▼
RacingBrain (SQLite)
     │
     ▼
XGBoost Handicapper
     │
     ▼
Kelly Criterion Bet Sizing
     │
     ▼
Responsible Gambling Guard
```

### Key Directories
| Directory | Purpose |
|-----------|---------|
| `grandpa_joe/brain/` | SQLite racing brain (12 tables), data ingestion |
| `grandpa_joe/models/` | ML handicapping: features, XGBoost, Kelly criterion |
| `grandpa_joe/ethics/` | Responsible gambling enforcement |
| `grandpa_joe/nexus/` | NEXUS client to ALFRED (wire format only, no router) |
| `grandpa_joe/api/` | FastAPI server, racing + NEXUS endpoints |
| `grandpa_joe/personality/` | Grandpa Joe commentary generator |

### PathManager
```python
from grandpa_joe.path_manager import PathManager
db_path = PathManager.BRAIN_DB  # CORRECT
# db_path = "C:/path/..."       # WRONG
```
Override root with `GRANDPA_JOE_HOME` env var.

### Graceful Degradation (Required Pattern)
```python
try:
    import xgboost
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False
```
ML, API, and NEXUS are all optional. Core brain always works.

## IP Protection Rules

**DO NOT** include any of these in this repo:
- CORTEX (5-layer forgetting) source code
- ULTRATHUNK (generative compression) source code
- Guardian (behavioral watermarking) source code
- NEXUSRouter or IntentTranslator internals

These are patent-pending and live in the ALFRED repo only.
GRANDPA_JOE consumes them via NEXUS REST API.

## Database Schema (12 tables)

tracks, horses, jockeys, trainers, races, entries, results,
past_performances, predictions, bets, handicapping_patterns,
gambling_session_log

## Environment Variables
```bash
GRANDPA_JOE_HOME="/custom/path"     # Override root directory
ALFRED_URL="http://127.0.0.1:8000"  # NEXUS bridge to ALFRED
NEXUS_SECRET="shared-secret"        # NEXUS authentication
EQUIBASE_API_KEY="..."              # Racing data provider
DAILY_LOSS_LIMIT="100.00"           # Responsible gambling
```

## Running Tests
```bash
python -m pytest tests/
# or individually
python tests/test_brain.py
python tests/test_kelly.py
```

## Personality
Grandpa Joe: Wise old handicapper. Folksy, knows the track.
"Now this number 5 horse, let me tell ya..."
Not sycophantic. Honest about bad bets. Cares about responsible gambling.

## Author
Daniel J Rita (BATDAN) | GxEum Technologies / CAMDAN Enterprizes
