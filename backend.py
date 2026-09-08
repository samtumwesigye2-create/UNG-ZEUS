"""ZEUS object-byte backend abstraction.

LocalFilesystemBackend is production-usable only when mounted on durable storage. The
interface deliberately isolates physical storage so S3-compatible, multi-site, or other
backends can be added without changing the ZEUS catalog/API contract.
"""
import gzip
import os
import shutil
from abc import ABC, abstractmethod


class StorageBackend(ABC):
    @abstractmethod
    def write_hot(self, storage_id: str, data: bytes): ...
    @abstractmethod
    def read(self, storage_id: str, tier: str) -> bytes: ...
    @abstractmethod
    def archive(self, storage_id: str): ...
    @abstractmethod
    def replicate(self, storage_id: str, tier: str, destination: str) -> str: ...


class LocalFilesystemBackend(StorageBackend):
    def __init__(self, base_dir: str):
        self.base_dir = os.path.abspath(base_dir)
        self.hot_dir = os.path.join(self.base_dir, "hot")
        self.archive_dir = os.path.join(self.base_dir, "archive")
        os.makedirs(self.hot_dir, exist_ok=True)
        os.makedirs(self.archive_dir, exist_ok=True)

    def _path(self, storage_id, tier):
        if tier == "hot":
            return os.path.join(self.hot_dir, storage_id)
        if tier == "archive":
            return os.path.join(self.archive_dir, storage_id + ".gz")
        raise ValueError(f"Unknown tier {tier}")

    def write_hot(self, storage_id, data):
        with open(self._path(storage_id, "hot"), "wb") as f:
            f.write(data)

    def read(self, storage_id, tier):
        path = self._path(storage_id, tier)
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        opener = gzip.open if tier == "archive" else open
        with opener(path, "rb") as f:
            return f.read()

    def archive(self, storage_id):
        src = self._path(storage_id, "hot")
        dst = self._path(storage_id, "archive")
        if not os.path.exists(src):
            raise FileNotFoundError(src)
        with open(src, "rb") as source, gzip.open(dst, "wb") as target:
            shutil.copyfileobj(source, target)
        os.remove(src)

    def replicate(self, storage_id, tier, destination):
        os.makedirs(destination, exist_ok=True)
        src = self._path(storage_id, tier)
        if not os.path.exists(src):
            raise FileNotFoundError(src)
        dst = os.path.join(destination, os.path.basename(src))
        shutil.copyfile(src, dst)
        return dst
