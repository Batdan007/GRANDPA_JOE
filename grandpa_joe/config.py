"""
Dataclass-based configuration for GRANDPA_JOE.
Loads from environment variables and optional config file.
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class RacingAPIKeys:
    """API keys for racing data providers."""
    equibase_api_key: str = ""
    drf_api_key: str = ""
    polygon_api_key: str = ""  # for any market data

    def has_any_key(self) -> bool:
        return any([self.equibase_api_key, self.drf_api_key])


@dataclass
class NexusSettings:
    """Settings for NEXUS bridge to ALFRED."""
    alfred_url: str = "http://127.0.0.1:8000"
    nexus_secret: str = ""
    timeout_seconds: int = 5
    enabled: bool = True
    backoff_max_seconds: int = 300


@dataclass
class GamblingLimits:
    """Responsible gambling limits."""
    daily_loss_limit: float = 100.0
    session_time_limit_minutes: int = 120
    max_single_bet: float = 50.0
    cooldown_after_loss_streak: int = 5
    require_confirmation_above: float = 25.0


@dataclass
class ModelSettings:
    """ML model configuration."""
    model_type: str = "xgboost"
    retrain_after_n_results: int = 500
    min_past_performances: int = 3
    confidence_threshold: float = 0.15
    default_kelly_fraction: float = 0.25


@dataclass
class ServerSettings:
    """API server configuration."""
    host: str = "127.0.0.1"
    port: int = 8100
    debug: bool = False
    workers: int = 1


@dataclass
class GrandpaJoeConfig:
    """Master configuration for GRANDPA_JOE."""
    api_keys: RacingAPIKeys = field(default_factory=RacingAPIKeys)
    nexus: NexusSettings = field(default_factory=NexusSettings)
    gambling: GamblingLimits = field(default_factory=GamblingLimits)
    model: ModelSettings = field(default_factory=ModelSettings)
    server: ServerSettings = field(default_factory=ServerSettings)
    debug: bool = False

    def _load_from_env(self):
        """Load configuration from environment variables."""
        # API keys
        self.api_keys.equibase_api_key = os.getenv("EQUIBASE_API_KEY", "")
        self.api_keys.drf_api_key = os.getenv("DRF_API_KEY", "")
        self.api_keys.polygon_api_key = os.getenv("POLYGON_API_KEY", "")

        # NEXUS
        self.nexus.alfred_url = os.getenv("ALFRED_URL", self.nexus.alfred_url)
        self.nexus.nexus_secret = os.getenv("NEXUS_SECRET", "")
        self.nexus.enabled = os.getenv("NEXUS_ENABLED", "true").lower() == "true"

        # Gambling limits
        if os.getenv("DAILY_LOSS_LIMIT"):
            self.gambling.daily_loss_limit = float(os.getenv("DAILY_LOSS_LIMIT"))
        if os.getenv("MAX_SINGLE_BET"):
            self.gambling.max_single_bet = float(os.getenv("MAX_SINGLE_BET"))
        if os.getenv("SESSION_TIME_LIMIT"):
            self.gambling.session_time_limit_minutes = int(os.getenv("SESSION_TIME_LIMIT"))

        # Server
        self.server.host = os.getenv("GRANDPA_JOE_HOST", self.server.host)
        if os.getenv("GRANDPA_JOE_PORT"):
            self.server.port = int(os.getenv("GRANDPA_JOE_PORT"))

        self.debug = os.getenv("GRANDPA_JOE_DEBUG", "false").lower() == "true"

    def _load_from_file(self, path: Optional[Path] = None):
        """Load configuration from JSON file."""
        from grandpa_joe.path_manager import PathManager
        config_path = path or PathManager.CONFIG_FILE
        if config_path.exists():
            try:
                with open(config_path) as f:
                    data = json.load(f)
                self._update_from_dict(data)
            except (json.JSONDecodeError, OSError):
                pass

    def _update_from_dict(self, data: dict):
        """Update config from dictionary."""
        if "nexus" in data:
            for k, v in data["nexus"].items():
                if hasattr(self.nexus, k):
                    setattr(self.nexus, k, v)
        if "gambling" in data:
            for k, v in data["gambling"].items():
                if hasattr(self.gambling, k):
                    setattr(self.gambling, k, v)
        if "model" in data:
            for k, v in data["model"].items():
                if hasattr(self.model, k):
                    setattr(self.model, k, v)
        if "server" in data:
            for k, v in data["server"].items():
                if hasattr(self.server, k):
                    setattr(self.server, k, v)

    def save_to_file(self, path: Optional[Path] = None):
        """Save configuration to JSON file."""
        from grandpa_joe.path_manager import PathManager
        config_path = path or PathManager.CONFIG_FILE
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    def to_dict(self) -> dict:
        from dataclasses import asdict
        return asdict(self)


# Singleton
_config: Optional[GrandpaJoeConfig] = None


def get_config() -> GrandpaJoeConfig:
    """Get or create the global config singleton."""
    global _config
    if _config is None:
        _config = GrandpaJoeConfig()
        # Load .env file if python-dotenv available
        try:
            from dotenv import load_dotenv
            from grandpa_joe.path_manager import PathManager
            env_path = PathManager.ENV_FILE
            if env_path.exists():
                load_dotenv(env_path)
        except ImportError:
            pass
        _config._load_from_env()
        _config._load_from_file()
    return _config


def reload_config() -> GrandpaJoeConfig:
    """Force reload configuration."""
    global _config
    _config = None
    return get_config()
