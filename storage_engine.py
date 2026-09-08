"""UNG-ZEUS dependency-free storage core."""
import gzip
import hashlib
import os
import shutil
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict


class IntegrityError(Exception):
    pass


class ObjectNotFoundError(Exception):
    pass


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class SimpleDictCatalog:
    def __init__(self):
        self._data: Dict[str, List[dict]] = {}

    def add_version(self, key, version, checksum, size, tier, created_at):
        self._data.setdefault(key, []).append({
            "key": key, "version": version, "checksum": checksum,
            "size": size, "tier": tier, "created_at": created_at,
        })

    def get_versions(self, key):
        return sorted(self._data.get(key, []), key=lambda v: v["version"])

    def get_latest(self, key):
        versions = self.get_versions(key)
        return versions[-1] if versions else None

    def update_tier(self, key, version, new_tier):
        for record in self._data.get(key, []):
            if record["version"] == version:
                record["tier"] = new_tier
                return
        raise ObjectNotFoundError(f"{key} v{version} not found")

    def all_keys(self):
        return list(self._data.keys())


class StorageEngine:
    def __init__(self, base_dir: str, catalog):
        self.base_dir = os.path.abspath(base_dir)
        self.hot_dir = os.path.join(self.base_dir, "hot")
        self.archive_dir = os.path.join(self.base_dir, "archive")
        os.makedirs(self.hot_dir, exist_ok=True)
        os.makedirs(self.archive_dir, exist_ok=True)
        self.catalog = catalog

    @staticmethod
    def _safe_key(key: str) -> str:
        if not key or key in {".", ".."}:
            raise ValueError("Object key must not be empty")
        # Keys are logical identifiers, never filesystem paths.
        return key.replace("\\", "__").replace("/", "__").replace("..", "__")

    def _hot_path(self, key, version):
        return os.path.join(self.hot_dir, f"{self._safe_key(key)}__v{version}")

    def _archive_path(self, key, version):
        return os.path.join(self.archive_dir, f"{self._safe_key(key)}__v{version}.gz")

    def put(self, key: str, data: bytes) -> dict:
        latest = self.catalog.get_latest(key)
        version = latest["version"] + 1 if latest else 1
        checksum = sha256_of(data)
        path = self._hot_path(key, version)
        with open(path, "wb") as f:
            f.write(data)
        created_at = datetime.now(timezone.utc)
        self.catalog.add_version(key, version, checksum, len(data), "hot", created_at)
        return {"key": key, "version": version, "checksum": checksum,
                "size": len(data), "tier": "hot", "created_at": created_at}

    def _resolve_version(self, key, version):
        if version is None:
            record = self.catalog.get_latest(key)
        else:
            record = next((v for v in self.catalog.get_versions(key) if v["version"] == version), None)
        if not record:
            raise ObjectNotFoundError(f"{key} (version={version}) not found")
        return record

    def _read_raw(self, key, version, tier):
        if tier == "hot":
            path = self._hot_path(key, version)
            opener = open
        elif tier == "archive":
            path = self._archive_path(key, version)
            opener = gzip.open
        else:
            raise ValueError(f"Unknown tier {tier}")
        if not os.path.exists(path):
            raise ObjectNotFoundError(f"{key} v{version} file missing")
        with opener(path, "rb") as f:
            return f.read()

    def get(self, key: str, version: Optional[int] = None) -> bytes:
        record = self._resolve_version(key, version)
        data = self._read_raw(key, record["version"], record["tier"])
        actual = sha256_of(data)
        if actual != record["checksum"]:
            raise IntegrityError(f"{key} v{record['version']}: checksum mismatch")
        return data

    def verify(self, key, version=None):
        try:
            self.get(key, version)
            return True
        except (IntegrityError, ObjectNotFoundError):
            return False

    def list_versions(self, key):
        return self.catalog.get_versions(key)

    def run_lifecycle(self, hot_days_threshold: int, now=None):
        now = now or datetime.now(timezone.utc)
        cutoff = now - timedelta(days=hot_days_threshold)
        moved = []
        for key in self.catalog.all_keys():
            for record in self.catalog.get_versions(key):
                if record["tier"] != "hot" or record["created_at"] > cutoff:
                    continue
                src = self._hot_path(key, record["version"])
                if not os.path.exists(src):
                    continue
                dst = self._archive_path(key, record["version"])
                with open(src, "rb") as source, gzip.open(dst, "wb") as target:
                    shutil.copyfileobj(source, target)
                os.remove(src)
                self.catalog.update_tier(key, record["version"], "archive")
                moved.append({"key": key, "version": record["version"]})
        return moved

    def replicate(self, key, version, replica_base_dir):
        record = self._resolve_version(key, version)
        os.makedirs(replica_base_dir, exist_ok=True)
        src = (self._hot_path(key, record["version"]) if record["tier"] == "hot"
               else self._archive_path(key, record["version"]))
        if not os.path.exists(src):
            raise ObjectNotFoundError(f"{key} v{record['version']}: source missing")
        dst = os.path.join(replica_base_dir, os.path.basename(src))
        shutil.copyfile(src, dst)
        return dst
