"""
Cross-platform path management for GRANDPA_JOE.
Follows ALFRED's PathManager pattern with GRANDPA_JOE_HOME override.
"""

import os
import platform
from pathlib import Path


def _get_platform_root() -> Path:
    """Get platform-specific root directory, overridable via GRANDPA_JOE_HOME."""
    env_home = os.environ.get("GRANDPA_JOE_HOME")
    if env_home:
        return Path(env_home)

    system = platform.system()
    if system == "Windows":
        return Path("C:/Drive")
    elif system == "Darwin":
        return Path.home() / "Library" / "Application Support" / "GrandpaJoe"
    else:
        return Path.home() / ".grandpa_joe"


class PathManager:
    """Centralized path management for GRANDPA_JOE."""

    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    DRIVE_ROOT = _get_platform_root()

    # Data
    DATA_DIR = DRIVE_ROOT / "data" / "grandpa_joe"
    BRAIN_DB = DATA_DIR / "racing_brain.db"
    INGESTION_DIR = DATA_DIR / "ingestion"

    # Models (trained XGBoost artifacts)
    MODELS_DIR = DATA_DIR / "models"

    # Config
    CONFIG_DIR = DRIVE_ROOT / "config" / "grandpa_joe"
    CONFIG_FILE = CONFIG_DIR / "config.json"
    ENV_FILE = PROJECT_ROOT / ".env"

    # Logs
    LOGS_DIR = DRIVE_ROOT / "logs" / "grandpa_joe"

    # Backups
    BACKUPS_DIR = DATA_DIR / "backups"

    @classmethod
    def ensure_all_paths(cls) -> dict:
        """Create all required directories. Returns dict of path: created."""
        results = {}
        dirs = [
            cls.DATA_DIR, cls.INGESTION_DIR, cls.MODELS_DIR,
            cls.CONFIG_DIR, cls.LOGS_DIR, cls.BACKUPS_DIR,
        ]
        for d in dirs:
            existed = d.exists()
            d.mkdir(parents=True, exist_ok=True)
            results[str(d)] = not existed
        return results

    @classmethod
    def verify_access(cls) -> bool:
        """Verify write access to data directory."""
        try:
            cls.DATA_DIR.mkdir(parents=True, exist_ok=True)
            test_file = cls.DATA_DIR / ".write_test"
            test_file.write_text("ok")
            test_file.unlink()
            return True
        except (OSError, PermissionError):
            return False

    @classmethod
    def get_platform_info(cls) -> dict:
        return {
            "system": platform.system(),
            "drive_root": str(cls.DRIVE_ROOT),
            "data_dir": str(cls.DATA_DIR),
            "brain_db": str(cls.BRAIN_DB),
            "models_dir": str(cls.MODELS_DIR),
            "project_root": str(cls.PROJECT_ROOT),
        }
