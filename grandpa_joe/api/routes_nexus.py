"""
NEXUS bridge API routes for GRANDPA_JOE.
Proxies queries to ALFRED via NEXUS protocol.
"""

import logging

logger = logging.getLogger(__name__)

try:
    from fastapi import APIRouter, Request, HTTPException
    from pydantic import BaseModel
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

if FASTAPI_AVAILABLE:
    router = APIRouter(prefix="/nexus", tags=["nexus"])

    class AskAlfredRequest(BaseModel):
        query: str
        context: dict = {}

    class SyncPrefsRequest(BaseModel):
        user_id: str = "default"
        preferences: dict = {}

    @router.get("/status")
    async def nexus_status(request: Request):
        """Check NEXUS connection to ALFRED."""
        nexus = getattr(request.app.state, "nexus", None)
        if nexus is None:
            return {
                "status": "disabled",
                "message": "NEXUS bridge not configured",
            }
        return {
            "status": "connected" if nexus.is_available() else "disconnected",
            **nexus.get_status(),
        }

    @router.post("/ask-alfred")
    async def ask_alfred(req: AskAlfredRequest, request: Request):
        """Send a query to ALFRED's brain via NEXUS."""
        nexus = getattr(request.app.state, "nexus", None)
        if nexus is None:
            raise HTTPException(503, "NEXUS bridge not configured")

        if not nexus.is_available():
            raise HTTPException(503, "ALFRED is not reachable")

        response = nexus.query_alfred(req.query, req.context)
        if response is None:
            raise HTTPException(502, "No response from ALFRED")

        return {"query": req.query, "alfred_response": response}

    @router.post("/sync-preferences")
    async def sync_preferences(req: SyncPrefsRequest, request: Request):
        """Sync betting preferences to/from ALFRED's brain."""
        nexus = getattr(request.app.state, "nexus", None)
        if nexus is None:
            raise HTTPException(503, "NEXUS bridge not configured")

        if not nexus.is_available():
            raise HTTPException(503, "ALFRED is not reachable")

        # Store each preference in ALFRED's knowledge base
        stored = 0
        for key, value in req.preferences.items():
            success = nexus.store_in_alfred(
                category="grandpa_joe_prefs",
                key=f"{req.user_id}:{key}",
                value=str(value),
            )
            if success:
                stored += 1

        return {
            "status": "ok",
            "preferences_synced": stored,
            "total_attempted": len(req.preferences),
        }

else:
    router = None
