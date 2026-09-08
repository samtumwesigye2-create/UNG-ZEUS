import base64
import binascii
import os
import tempfile
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy.orm import Session
from db import get_db
from ingestion_db import IngestionJob, enqueue
from integrations.pulsar import PulsarAuthError, PulsarReplayError, parse_envelope, verify_signature

router = APIRouter(prefix="/integrations/pulsar", tags=["UNG-PULSAR"])
STAGING_BASE_DIR = os.environ.get("STAGING_BASE_DIR", "./zeus_staging")
MAX_PULSAR_PAYLOAD_BYTES = int(os.environ.get("MAX_PULSAR_PAYLOAD_BYTES", str(25 * 1024 * 1024)))

@router.post("/ingest", status_code=202)
async def pulsar_ingest(request: Request, db: Session = Depends(get_db),
                        x_pulsar_timestamp: str = Header(default=""),
                        x_pulsar_event_id: str = Header(default=""),
                        x_pulsar_signature: str = Header(default="")):
    body = await request.body()
    if not x_pulsar_event_id:
        raise HTTPException(400, "X-Pulsar-Event-Id is required")
    try:
        verify_signature(body, x_pulsar_timestamp, x_pulsar_event_id, x_pulsar_signature)
        env = parse_envelope(body)
    except PulsarReplayError as exc:
        raise HTTPException(409, str(exc))
    except (PulsarAuthError, KeyError, ValueError) as exc:
        raise HTTPException(401, str(exc))
    if env.event_id != x_pulsar_event_id:
        raise HTTPException(400, "PULSAR event id mismatch")
    # Idempotency: a relay retry returns the existing job rather than storing twice.
    logical_key = f"{env.namespace}/{env.object_key}"
    marker = f"pulsar:{env.event_id}:{logical_key}"
    existing = db.query(IngestionJob).filter(IngestionJob.object_key == marker).first()
    if existing:
        return {"accepted": True, "duplicate": True, "event_id": env.event_id, "job_id": existing.id, "status": existing.status}
    try:
        payload = base64.b64decode(env.payload_b64, validate=True)
    except (binascii.Error, ValueError):
        raise HTTPException(422, "payload_b64 is invalid")
    if not payload:
        raise HTTPException(422, "PULSAR payload is empty")
    if len(payload) > MAX_PULSAR_PAYLOAD_BYTES:
        raise HTTPException(413, "PULSAR inline payload exceeds configured limit; use multipart/object handoff")
    os.makedirs(STAGING_BASE_DIR, exist_ok=True)
    fd, path = tempfile.mkstemp(prefix="pulsar-zeus-", dir=STAGING_BASE_DIR)
    try:
        with os.fdopen(fd, "wb") as target:
            target.write(payload)
            target.flush()
            os.fsync(target.fileno())
        # Store the PULSAR event marker as the queue key for idempotency. Worker resolves the logical key sidecar.
        with open(path + ".key", "w", encoding="utf-8") as sidecar:
            sidecar.write(logical_key)
        job = enqueue(db, marker, path)
        return {"accepted": True, "duplicate": False, "event_id": env.event_id, "trace_id": env.trace_id,
                "job_id": job.id, "status": job.status, "key": logical_key, "bytes": len(payload), "priority": env.priority}
    except Exception:
        for candidate in (path, path + ".key"):
            try: os.remove(candidate)
            except OSError: pass
        raise

@router.get("/jobs/{job_id}")
def pulsar_job(job_id: int, db: Session = Depends(get_db)):
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(404, "Ingestion job not found")
    return {"job_id": job.id, "status": job.status, "attempts": job.attempts, "error": job.error}
