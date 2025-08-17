import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class BucketExistsRequest:
    """
    Request model to check whether a bucket exists.
    """

    bucket: str


@dataclass(frozen=True)
class GetLockInfoRequest:
    """
    Request model to fetch existing lock information for a specific object key.
    """

    bucket: str
    object_key: str


@dataclass(frozen=True)
class AcquireLockRequest:
    """
    Request model to acquire (create) a new lock.

    Attributes:
        bucket: Target bucket name.
        object_key: Object key representing the lock.
        owner: Lock owner identifier. If not provided, a UUID will be generated.
        expires_sec: Time-to-live of the lock in seconds.
        force: Whether to force creation even if a lock may already exist.
    """

    bucket: str
    object_key: str
    owner: str | None = None
    expires_sec: int = 20
    force: bool = False

    def to_metadata(self) -> dict:
        """
        Convert this request into a metadata dictionary for the storage object.

        Returns:
            dict: A dictionary containing expires_sec and lock_owner fields.
        """
        owner = self.owner or uuid.uuid4().hex
        return {
            "expires_sec": str(self.expires_sec),
            "lock_owner": owner,
        }


@dataclass(frozen=True)
class UpdateLockRequest:
    """
    Request model to update (refresh/extend) an existing lock.

    Attributes:
        bucket: Target bucket name.
        object_key: Object key representing the lock.
        metageneration: Metageneration required for conditional update.
        owner: Lock owner identifier.
        expires_sec: New time-to-live of the lock in seconds.
        force: Whether to bypass conditional update constraints.
    """

    bucket: str
    object_key: str
    metageneration: int
    owner: str
    expires_sec: int = 20
    force: bool = False

    def to_metadata(self) -> dict:
        """
        Convert this request into a metadata dictionary for the storage object.

        Returns:
            dict: A dictionary containing expires_sec and lock_owner fields.
        """
        return {
            "expires_sec": str(self.expires_sec),
            "lock_owner": self.owner,
        }


@dataclass(frozen=True)
class ReleaseLockRequest:
    """
    Request model to release (delete) an acquired lock.

    Attributes:
        bucket: Target bucket name.
        object_key: Object key representing the lock.
        generation: Object generation for conditional deletion.
        metageneration: Object metageneration for conditional deletion.
    """

    bucket: str
    object_key: str
    generation: int
    metageneration: int


@dataclass(frozen=True)
class LockResponse:
    """
    Model representing the lock state returned by the storage backend.

    Attributes:
        bucket: Bucket name.
        object_key: Object key representing the lock.
        generation: Object generation.
        metageneration: Object metageneration.
        lock_owner: Identifier of the lock owner.
        locked_at: UTC timestamp when the lock was set.
        expires_sec: Time-to-live of the lock in seconds.
    """

    bucket: str
    object_key: str
    generation: int
    metageneration: int
    lock_owner: str
    locked_at: datetime
    expires_sec: int

    @property
    def expires_at(self) -> datetime:
        """
        Compute the UTC timestamp when the lock will expire.

        Returns:
            datetime: locked_at + expires_sec in UTC.
        """
        return self.locked_at + timedelta(seconds=self.expires_sec)

    def is_expired(self) -> bool:
        """
        Determine whether the lock is already expired.

        Returns:
            bool: True if the current time is past expires_at; otherwise False.
        """
        return self.expires_at < datetime.now(timezone.utc)

    def to_metadata(self) -> dict:
        """
        Convert the current lock state into a storage metadata dictionary.

        Returns:
            dict: A dictionary containing expires_sec and lock_owner fields.
        """
        return {
            "expires_sec": str(self.expires_sec),
            "lock_owner": self.lock_owner,
        }

    def can_be_acquire_by(self, owner: str):
        """
        Check whether the given owner can acquire (update) this lock.

        The same owner can always acquire. A different owner can acquire only
        when the lock is expired.

        Args:
            owner: Owner identifier to check.

        Returns:
            bool: True if acquisition is allowed; otherwise False.
        """
        if owner == self.lock_owner:
            return True
        else:
            return self.is_expired()
