"""
Clean-room NEXUS message types for GRANDPA_JOE.
Only the wire format — NO router, translator, or capability classes.
Those are ALFRED-internal / patent-adjacent.
"""

import hashlib
import hmac
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class MessageType(str, Enum):
    """NEXUS message types."""
    QUERY = "query"
    RESPONSE = "response"
    COMMAND = "command"
    ACKNOWLEDGE = "acknowledge"
    STATUS = "status"
    ERROR = "error"


class IntentType(str, Enum):
    """NEXUS intent types. Wire values match ALFRED core/nexus.py enum."""
    INFORMATION_REQUEST = "info_request"
    TASK_EXECUTION = "task_execute"
    COLLABORATION = "collaborate"
    STATUS_CHECK = "status"
    RESOURCE_REQUEST = "resource"


def build_message(
    message_type: MessageType,
    intent: IntentType,
    sender_id: str,
    receiver_id: str,
    payload: Dict[str, Any],
    reply_to: Optional[str] = None,
    priority: int = 5,
    ttl: int = 300,
) -> Dict:
    """
    Build a NEXUSMessage dict for HTTP transport.
    Matches ALFRED's NEXUSMessage.to_dict() wire format.
    """
    msg_id = f"MSG-{uuid.uuid4().hex[:12]}"
    return {
        "id": msg_id,
        "message_type": message_type.value,
        "intent": intent.value,
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "payload": payload,
        "timestamp": datetime.now().isoformat(),
        "reply_to": reply_to,
        "ttl": ttl,
        "priority": priority,
        "signature": None,
        "metadata": {
            "source": "grandpa_joe",
            "version": "0.1.0",
        },
    }


def sign_message(message: Dict, secret: str) -> str:
    """
    Sign a NEXUS message with HMAC-SHA256.
    Matches ALFRED's NEXUSMessage.sign() format.
    """
    sign_data = f"{message['id']}:{message['sender_id']}:{message['timestamp']}"
    signature = hmac.new(
        secret.encode(), sign_data.encode(), hashlib.sha256
    ).hexdigest()
    message["signature"] = signature
    return signature


def verify_signature(message: Dict, secret: str) -> bool:
    """Verify a NEXUS message signature."""
    expected = sign_message(dict(message), secret)
    return hmac.compare_digest(
        message.get("signature", ""), expected
    )
