from sqlalchemy import Column, Integer, String, DateTime, UniqueConstraint
from sqlalchemy.orm import Session
from db import Base


class ObjectVersion(Base):
    __tablename__ = "zeus_object_versions"
    __table_args__ = (UniqueConstraint("key", "version", name="uq_zeus_key_version"),)
    id = Column(Integer, primary_key=True)
    key = Column(String, index=True, nullable=False)
    version = Column(Integer, nullable=False)
    checksum = Column(String, nullable=False)
    size = Column(Integer, nullable=False)
    tier = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)


class DbCatalog:
    def __init__(self, db: Session):
        self.db = db

    def add_version(self, key, version, checksum, size, tier, created_at):
        self.db.add(ObjectVersion(key=key, version=version, checksum=checksum,
                                  size=size, tier=tier, created_at=created_at))
        self.db.commit()

    def get_versions(self, key):
        rows = self.db.query(ObjectVersion).filter(ObjectVersion.key == key).order_by(ObjectVersion.version.asc()).all()
        return [self._to_dict(r) for r in rows]

    def get_latest(self, key):
        row = self.db.query(ObjectVersion).filter(ObjectVersion.key == key).order_by(ObjectVersion.version.desc()).first()
        return self._to_dict(row) if row else None

    def update_tier(self, key, version, new_tier):
        row = self.db.query(ObjectVersion).filter(ObjectVersion.key == key, ObjectVersion.version == version).first()
        if not row:
            raise ValueError(f"{key} v{version} not found")
        row.tier = new_tier
        self.db.commit()

    def all_keys(self):
        return [r[0] for r in self.db.query(ObjectVersion.key).distinct().all()]

    @staticmethod
    def _to_dict(row):
        return {"key": row.key, "version": row.version, "checksum": row.checksum,
                "size": row.size, "tier": row.tier, "created_at": row.created_at}
