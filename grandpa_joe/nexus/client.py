"""
NEXUS client for GRANDPA_JOE.
HTTP client that speaks NEXUS wire format to ALFRED.
Graceful degradation — if ALFRED is offline, everything still works locally.
"""

import logging
import time
import uuid
from typing import Any, Dict, Optional

from grandpa_joe.nexus.messages import (
    MessageType, IntentType, build_message, sign_message,
)

logger = logging.getLogger(__name__)

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


class NexusClient:
    """
    HTTP client for NEXUS communication with ALFRED.
    Posts NEXUSMessage JSON to ALFRED's /v1/nexus/message endpoint.
    """

    AGENT_ID = f"GRANDPA_JOE-{uuid.uuid4().hex[:8]}"

    def __init__(self, alfred_url: str = "http://127.0.0.1:8000",
                 secret: str = "", timeout: int = 5):
        self.alfred_url = alfred_url.rstrip("/")
        self.secret = secret
        self.timeout = timeout

        self._available: Optional[bool] = None
        self._last_check: float = 0
        self._backoff: float = 30  # seconds between availability checks
        self._max_backoff: float = 300

    def _should_check(self) -> bool:
        """Should we check ALFRED availability?"""
        if self._available is None:
            return True
        return time.time() - self._last_check > self._backoff

    def is_available(self) -> bool:
        """Check if ALFRED is reachable. Cached with backoff."""
        if not HTTPX_AVAILABLE:
            return False

        if not self._should_check():
            return self._available or False

        self._last_check = time.time()
        try:
            with httpx.Client(timeout=self.timeout) as client:
                resp = client.get(f"{self.alfred_url}/health")
                self._available = resp.status_code == 200
                if self._available:
                    self._backoff = 30  # reset on success
                    logger.debug("ALFRED NEXUS connection: available")
                return self._available
        except Exception:
            self._available = False
            self._backoff = min(self._backoff * 2, self._max_backoff)
            logger.debug(f"ALFRED not reachable, backoff={self._backoff}s")
            return False

    def _send_message(self, message: Dict) -> Optional[Dict]:
        """Send a NEXUS message to ALFRED and return the response."""
        if not HTTPX_AVAILABLE:
            logger.debug("httpx not installed — NEXUS unavailable")
            return None

        if not self.is_available():
            return None

        # Sign message
        if self.secret:
            sign_message(message, self.secret)

        try:
            with httpx.Client(timeout=self.timeout) as client:
                resp = client.post(
                    f"{self.alfred_url}/v1/nexus/message",
                    json=message,
                    headers={"X-Nexus-Secret": self.secret} if self.secret else {},
                )
                if resp.status_code == 200:
                    return resp.json()
                else:
                    logger.warning(f"NEXUS message failed: {resp.status_code}")
                    return None
        except Exception as e:
            logger.debug(f"NEXUS send failed: {e}")
            self._available = False
            return None

    def query_alfred(self, query: str, context: Optional[Dict] = None) -> Optional[str]:
        """
        Ask ALFRED a question via NEXUS.

        Args:
            query: The question to ask
            context: Optional context dict

        Returns:
            ALFRED's response string, or None if unavailable
        """
        message = build_message(
            message_type=MessageType.QUERY,
            intent=IntentType.INFORMATION_REQUEST,
            sender_id=self.AGENT_ID,
            receiver_id="ALFRED",
            payload={"query": query, "context": context or {}},
        )
        response = self._send_message(message)
        if response and "payload" in response:
            return response["payload"].get("response")
        return None

    def store_in_alfred(self, category: str, key: str, value: str) -> bool:
        """
        Ask ALFRED to store knowledge.

        Args:
            category: Knowledge category
            key: Knowledge key
            value: Knowledge value

        Returns:
            True if stored successfully
        """
        message = build_message(
            message_type=MessageType.COMMAND,
            intent=IntentType.TASK_EXECUTION,
            sender_id=self.AGENT_ID,
            receiver_id="ALFRED",
            payload={
                "action": "knowledge_store",
                "category": category,
                "key": key,
                "value": value,
            },
        )
        response = self._send_message(message)
        return response is not None and response.get("message_type") == "acknowledge"

    def get_from_alfred(self, category: str, key: str) -> Optional[str]:
        """
        Retrieve knowledge from ALFRED's brain.

        Args:
            category: Knowledge category
            key: Knowledge key

        Returns:
            Value string, or None if not found/unavailable
        """
        message = build_message(
            message_type=MessageType.QUERY,
            intent=IntentType.RESOURCE_REQUEST,
            sender_id=self.AGENT_ID,
            receiver_id="ALFRED",
            payload={
                "action": "knowledge_recall",
                "category": category,
                "key": key,
            },
        )
        response = self._send_message(message)
        if response and "payload" in response:
            return response["payload"].get("value")
        return None

    def cortex_capture(self, content: str, importance: float = 5.0,
                        topic: str = "", metadata: Optional[Dict] = None) -> bool:
        """
        Ask ALFRED's CORTEX to capture an item into its memory tiers.

        Mirrors ALFRED's cortex.capture(content, importance, topic, metadata).
        CORTEX auto-assigns a layer (FLASH / WORKING / SHORT_TERM / LONG_TERM /
        ARCHIVE) based on importance and access patterns — we only hint.

        Returns True if ALFRED acknowledged. False on any failure (fire-and-forget
        safe — caller doesn't need to handle offline ALFRED).
        """
        message = build_message(
            message_type=MessageType.COMMAND,
            intent=IntentType.TASK_EXECUTION,
            sender_id=self.AGENT_ID,
            receiver_id="ALFRED",
            payload={
                "action": "cortex_store",
                "content": content,
                "importance": importance,
                "topic": topic,
                "metadata": metadata or {},
            },
        )
        response = self._send_message(message)
        if not response:
            return False
        return response.get("message_type") in ("acknowledge", "response")

    def ping(self) -> bool:
        """Simple ping to check ALFRED is reachable."""
        # Force a fresh check
        self._last_check = 0
        return self.is_available()

    def get_status(self) -> Dict:
        """Get NEXUS connection status."""
        return {
            "alfred_url": self.alfred_url,
            "available": self._available,
            "agent_id": self.AGENT_ID,
            "httpx_installed": HTTPX_AVAILABLE,
            "backoff_seconds": self._backoff,
        }
