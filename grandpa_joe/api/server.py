"""
FastAPI server for GRANDPA_JOE.
Mobile-ready REST API on port 8100.
"""

import logging

from grandpa_joe import __version__
from grandpa_joe.brain import RacingBrain
from grandpa_joe.config import get_config
from grandpa_joe.ethics.responsible_gambling import ResponsibleGamblingGuard
from grandpa_joe.path_manager import PathManager

logger = logging.getLogger(__name__)

try:
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

if FASTAPI_AVAILABLE:
    app = FastAPI(
        title="Grandpa Joe - Horse Racing Handicapper",
        description="The wise old handicapper's API. Picks, bets, and track wisdom.",
        version=__version__,
    )

    # CORS for mobile apps
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ========================================================================
    # Startup
    # ========================================================================

    @app.on_event("startup")
    async def startup():
        """Initialize brain, model, and ethics guard on server start."""
        PathManager.ensure_all_paths()
        config = get_config()

        app.state.brain = RacingBrain()
        app.state.config = config
        app.state.guard = ResponsibleGamblingGuard(config.gambling, app.state.brain)

        # Try loading handicapper
        try:
            from grandpa_joe.models.handicapper import GrandpaJoeHandicapper
            app.state.handicapper = GrandpaJoeHandicapper(
                app.state.brain, config.model
            )
            app.state.model_loaded = app.state.handicapper.model is not None
        except ImportError:
            app.state.handicapper = None
            app.state.model_loaded = False

        # Try NEXUS client
        try:
            from grandpa_joe.nexus.client import NexusClient
            if config.nexus.enabled:
                app.state.nexus = NexusClient(
                    config.nexus.alfred_url,
                    config.nexus.nexus_secret,
                    config.nexus.timeout_seconds,
                )
            else:
                app.state.nexus = None
        except ImportError:
            app.state.nexus = None

        logger.info(f"Grandpa Joe API started on port {config.server.port}")

    # ========================================================================
    # Health & Info
    # ========================================================================

    from grandpa_joe.api.models import HealthResponse, StatsResponse

    @app.get("/health", response_model=HealthResponse)
    async def health():
        return HealthResponse(
            status="ok",
            version=__version__,
            brain_connected=True,
            model_loaded=getattr(app.state, "model_loaded", False),
            nexus_available=getattr(app.state, "nexus", None) is not None,
        )

    @app.get("/api")
    async def api_info():
        return {
            "name": "Grandpa Joe",
            "version": __version__,
            "description": "Horse Racing Handicapping API",
            "endpoints": {
                "health": "/health",
                "stats": "/v1/stats",
                "handicap": "POST /v1/handicap/{race_id}",
                "bets_suggest": "POST /v1/bets/suggest",
                "bets_record": "POST /v1/bets/record",
                "races": "GET /v1/races/{race_id}",
                "search": "POST /v1/search",
            },
        }

    @app.get("/v1/stats", response_model=StatsResponse)
    async def get_stats():
        stats = app.state.brain.get_memory_stats()
        return StatsResponse(**stats)

    # ========================================================================
    # Racing routes (imported)
    # ========================================================================

    from grandpa_joe.api.routes_racing import router as racing_router
    app.include_router(racing_router, prefix="/v1")

    from grandpa_joe.api.routes_nexus import router as nexus_router
    if nexus_router:
        app.include_router(nexus_router, prefix="/v1")

else:
    # Stub if FastAPI not installed
    app = None
