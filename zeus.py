import os
import tempfile
from typing import Optional
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel
from sqlalchemy.orm import Session
from auth import require_admin_key
from catalog_db import DbCatalog
from db import get_db
from ingestion_db import IngestionJob, enqueue
from storage_engine import IntegrityError, ObjectNotFoundError, StorageEngine

router = APIRouter(prefix="/zeus", tags=["UNG-ZEUS"])
STORAGE_BASE_DIR = os.environ.get("STORAGE_BASE_DIR", "./zeus_data")
STAGING_BASE_DIR = os.environ.get("STAGING_BASE_DIR", "./zeus_staging")
REPLICA_BASE_DIR = os.environ.get("REPLICA_BASE_DIR")
DEFAULT_HOT_DAYS = int(os.environ.get("HOT_DAYS_THRESHOLD", "90"))


def _engine(db: Session):
    return StorageEngine(STORAGE_BASE_DIR, DbCatalog(db))


class LifecycleRunIn(BaseModel):
    hot_days_threshold: Optional[int] = None


@router.post("/objects/{key:path}")
def upload_object(key: str, file: UploadFile = File(...), db: Session = Depends(get_db), _=Depends(require_admin_key)):
    data = file.file.read()
    if not data:
        raise HTTPException(400, "Uploaded file is empty")
    record = _engine(db).put(key, data)
    return {k: record[k] for k in ("key", "version", "checksum", "size", "tier")}


@router.post("/ingest/{key:path}", status_code=202)
def enqueue_object(key: str, file: UploadFile = File(...), db: Session = Depends(get_db), _=Depends(require_admin_key)):
    """Durably stage an upload and queue it for asynchronous ingestion."""
    os.makedirs(STAGING_BASE_DIR, exist_ok=True)
    fd, path = tempfile.mkstemp(prefix="zeus-", dir=STAGING_BASE_DIR)
    size = 0
    try:
        with os.fdopen(fd, "wb") as target:
            while True:
                chunk = file.file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                target.write(chunk)
        if size == 0:
            os.remove(path)
            raise HTTPException(400, "Uploaded file is empty")
        job = enqueue(db, key, path)
        return {"job_id": job.id, "key": key, "status": job.status, "size": size}
    except HTTPException:
        raise
    except Exception:
        try:
            os.remove(path)
        except OSError:
            pass
        raise


@router.get("/ingest/jobs/{job_id}")
def ingestion_status(job_id: int, db: Session = Depends(get_db), _=Depends(require_admin_key)):
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(404, "Ingestion job not found")
    return {"job_id": job.id, "key": job.object_key, "status": job.status,
            "attempts": job.attempts, "error": job.error,
            "created_at": job.created_at.isoformat(), "updated_at": job.updated_at.isoformat()}


@router.get("/objects/{key:path}/versions")
def list_versions(key: str, db: Session = Depends(get_db), _=Depends(require_admin_key)):
    versions = _engine(db).list_versions(key)
    if not versions:
        raise HTTPException(404, "Object not found")
    return [{"version": v["version"], "checksum": v["checksum"], "size": v["size"],
             "tier": v["tier"], "created_at": v["created_at"].isoformat() if v["created_at"] else None}
            for v in versions]


@router.get("/objects/{key:path}/verify")
def verify_object(key: str, version: Optional[int] = None, db: Session = Depends(get_db), _=Depends(require_admin_key)):
    return {"key": key, "version": version, "intact": _engine(db).verify(key, version)}


@router.post("/objects/{key:path}/replicate")
def replicate_object(key: str, version: Optional[int] = None, db: Session = Depends(get_db), _=Depends(require_admin_key)):
    if not REPLICA_BASE_DIR:
        raise HTTPException(503, "REPLICA_BASE_DIR not configured")
    try:
        dst = _engine(db).replicate(key, version, REPLICA_BASE_DIR)
    except ObjectNotFoundError:
        raise HTTPException(404, "Object not found")
    return {"key": key, "version": version, "replicated_to": dst}


@router.post("/lifecycle/run")
def run_lifecycle(payload: LifecycleRunIn, db: Session = Depends(get_db), _=Depends(require_admin_key)):
    threshold = payload.hot_days_threshold if payload.hot_days_threshold is not None else DEFAULT_HOT_DAYS
    moved = _engine(db).run_lifecycle(threshold)
    return {"hot_days_threshold": threshold, "archived_count": len(moved), "archived": moved}


@router.get("/objects/{key:path}")
def download_object(key: str, version: Optional[int] = None, db: Session = Depends(get_db), _=Depends(require_admin_key)):
    try:
        data = _engine(db).get(key, version)
    except ObjectNotFoundError:
        raise HTTPException(404, "Object not found")
    except IntegrityError as exc:
        raise HTTPException(409, f"Integrity check failed: {exc}")
    return Response(content=data, media_type="application/octet-stream")
