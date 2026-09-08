"""ZEUS durable ingestion worker.

Run separately from the API process. A future PULSAR adapter can feed the same storage
contract while this worker remains a safe fallback/recovery path.
"""
import os
import time
from catalog_db import DbCatalog
from db import Base, SessionLocal, engine
from ingestion_db import IngestionJob, claim_next, fail, finish  # noqa: F401
from storage_engine import StorageEngine

STORAGE_BASE_DIR = os.environ.get("STORAGE_BASE_DIR", "./zeus_data")
POLL_SECONDS = float(os.environ.get("INGESTION_POLL_SECONDS", "1"))
MAX_ATTEMPTS = int(os.environ.get("INGESTION_MAX_ATTEMPTS", "5"))


def process_one():
    db = SessionLocal()
    try:
        job = claim_next(db)
        if not job:
            return False
        try:
            with open(job.staging_path, "rb") as f:
                data = f.read()
            StorageEngine(STORAGE_BASE_DIR, DbCatalog(db)).put(job.object_key, data)
            try:
                os.remove(job.staging_path)
            except FileNotFoundError:
                pass
            finish(db, job)
        except Exception as exc:
            fail(db, job, str(exc), MAX_ATTEMPTS)
        return True
    finally:
        db.close()


def main():
    Base.metadata.create_all(bind=engine)
    while True:
        if not process_one():
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
