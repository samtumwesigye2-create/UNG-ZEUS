# UNG-ZEUS

National Data Storage Platform for the Uganda National Grid ecosystem.

ZEUS is responsible for durable data storage, versioning, integrity verification, lifecycle tiering, replication, caching and object placement. It is intentionally separate from UNG-VAULT: ZEUS stores data; VAULT provides cryptographic/secrets services.

## Foundation

The initial core implements:

- content-addressed/versioned object storage — uploads never overwrite prior versions
- SHA-256 verification on reads
- hot-to-archive lifecycle tiering using gzip
- file-level replica copies
- path-safe object keys
- PostgreSQL-backed metadata catalog for the API layer

## Target architecture

The completed platform will add PULSAR asynchronous ingestion, a cache layer, durable object-storage backends, network/multi-server replication, NEXUS APIs/events, JANUS identity/RBAC/service authorization, and VAULT-backed secrets/cryptographic integration.

## API

All current object endpoints require `X-Admin-Key` until JANUS integration replaces the bootstrap shared-key mechanism.

- `POST /zeus/objects/{key}`
- `GET /zeus/objects/{key}?version=N`
- `GET /zeus/objects/{key}/versions`
- `GET /zeus/objects/{key}/verify?version=N`
- `POST /zeus/lifecycle/run`
- `POST /zeus/objects/{key}/replicate?version=N`
- `GET /health`

## Run

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8080
```

Run the dependency-free storage-engine acceptance suite:

```bash
python3 test_storage_engine.py
```
