"""Durable ZEUS ingestion-job catalog.

This is the local durable queue contract used until the external PULSAR event transport
is wired. Jobs survive process restarts because state is held in PostgreSQL/SQLAlchemy.
"""
from datetime import datetime, timezone
from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.orm import Session
from db import Base


class IngestionJob(Base):
    __tablename__ = "zeus_ingestion_jobs"
    id = Column(Integer, primary_key=True)
    object_key = Column(String, nullable=False, index=True)
    staging_path = Column(Text, nullable=False)
    status = Column(String, nullable=False, default="queued", index=True)
    attempts = Column(Integer, nullable=False, default=0)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


def enqueue(db: Session, object_key: str, staging_path: str):
    now = datetime.now(timezone.utc)
    job = IngestionJob(object_key=object_key, staging_path=staging_path, status="queued",
                       attempts=0, created_at=now, updated_at=now)
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def claim_next(db: Session):
    # with_for_update(skip_locked=True) lets multiple Postgres workers safely compete.
    query = db.query(IngestionJob).filter(IngestionJob.status == "queued").order_by(IngestionJob.id.asc())
    try:
        job = query.with_for_update(skip_locked=True).first()
    except Exception:
        db.rollback()
        job = query.first()  # SQLite/dev fallback.
    if not job:
        return None
    job.status = "processing"
    job.attempts += 1
    job.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(job)
    return job


def finish(db: Session, job: IngestionJob):
    job.status = "complete"
    job.error = None
    job.updated_at = datetime.now(timezone.utc)
    db.commit()


def fail(db: Session, job: IngestionJob, error: str, max_attempts: int = 5):
    job.status = "queued" if job.attempts < max_attempts else "failed"
    job.error = error[:4000]
    job.updated_at = datetime.now(timezone.utc)
    db.commit()
