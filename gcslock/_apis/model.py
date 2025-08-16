import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class BucketExistsRequest:
    bucket: str


@dataclass(frozen=True)
class GetLockInfoRequest:
    bucket: str
    object_key: str


@dataclass(frozen=True)
class AcquireLockRequest:
    bucket: str
    object_key: str
    owner: str | None = None
    expires_sec: int = 20
    force: bool = False

    def to_metadata(self) -> dict:
        owner = self.owner or uuid.uuid4().hex
        return {
            "expires_sec": str(self.expires_sec),
            "lock_owner": owner,
        }


@dataclass(frozen=True)
class UpdateLockRequest:
    bucket: str
    object_key: str
    metageneration: int
    owner: str
    expires_sec: int = 20
    force: bool = False

    def to_metadata(self) -> dict:
        return {
            "expires_sec": str(self.expires_sec),
            "lock_owner": self.owner,
        }


@dataclass(frozen=True)
class ReleaseLockRequest:
    bucket: str
    object_key: str
    generation: int
    metageneration: int


@dataclass(frozen=True)
class LockResponse:
    bucket: str
    object_key: str
    generation: int
    metageneration: int
    lock_owner: str
    locked_at: datetime
    expires_sec: int

    @property
    def expires_at(self) -> datetime:
        return self.locked_at + timedelta(seconds=self.expires_sec)

    def is_expired(self) -> bool:
        return self.expires_at < datetime.now(timezone.utc)

    def to_metadata(self) -> dict:
        return {
            "expires_sec": str(self.expires_sec),
            "lock_owner": self.lock_owner,
        }

    def can_be_acquire_by(self, owner: str):
        if owner == self.lock_owner:
            return True
        else:
            return self.is_expired()
