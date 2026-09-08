"""UNG-PULSAR -> UNG-ZEUS ingestion boundary.

Accepts signed relay envelopes and converts them into ZEUS durable ingestion jobs.
PULSAR remains non-blocking: ZEUS acknowledges after the job is durably queued,
not after downstream storage/replication completes.
"""
import hashlib
import hmac
import json
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

PULSAR_SHARED_SECRET = os.environ.get("PULSAR_SHARED_SECRET", "")
PULSAR_MAX_CLOCK_SKEW_SECONDS = int(os.environ.get("PULSAR_MAX_CLOCK_SKEW_SECONDS", "300"))

class PulsarAuthError(Exception): pass
class PulsarReplayError(Exception): pass

@dataclass
class PulsarEnvelope:
    event_id: str
    source: str
    object_key: str
    namespace: str
    payload_b64: str
    content_type: str
    classification: str
    priority: int
    trace_id: str
    timestamp: int
    metadata: Dict[str, Any]


def canonical_bytes(body: bytes, timestamp: str, event_id: str) -> bytes:
    return timestamp.encode() + b"." + event_id.encode() + b"." + body


def verify_signature(body: bytes, timestamp: str, event_id: str, signature: str) -> None:
    if not PULSAR_SHARED_SECRET:
        raise PulsarAuthError("PULSAR_SHARED_SECRET is not configured")
    try:
        ts = int(timestamp)
    except ValueError as exc:
        raise PulsarAuthError("Invalid PULSAR timestamp") from exc
    if abs(int(time.time()) - ts) > PULSAR_MAX_CLOCK_SKEW_SECONDS:
        raise PulsarReplayError("PULSAR envelope is outside the replay window")
    expected = hmac.new(PULSAR_SHARED_SECRET.encode(), canonical_bytes(body, timestamp, event_id), hashlib.sha256).hexdigest()
    supplied = signature.removeprefix("sha256=")
    if not hmac.compare_digest(expected, supplied):
        raise PulsarAuthError("Invalid PULSAR signature")


def parse_envelope(body: bytes) -> PulsarEnvelope:
    raw = json.loads(body)
    return PulsarEnvelope(
        event_id=str(raw.get("event_id") or uuid.uuid4()),
        source=str(raw["source"]), object_key=str(raw["object_key"]),
        namespace=str(raw.get("namespace") or raw["source"]).lower(),
        payload_b64=str(raw["payload_b64"]), content_type=str(raw.get("content_type") or "application/octet-stream"),
        classification=str(raw.get("classification") or "internal"),
        priority=max(0, min(100, int(raw.get("priority", 50)))),
        trace_id=str(raw.get("trace_id") or ""), timestamp=int(raw["timestamp"]),
        metadata=dict(raw.get("metadata") or {}),
    )
