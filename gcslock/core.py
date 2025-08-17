import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from google.auth import default
from google.auth.credentials import Credentials

from ._apis.accessor import Accessor, RestAccessor
from ._apis.model import (
    AcquireLockRequest,
    BucketExistsRequest,
    GetLockInfoRequest,
    LockResponse,
    ReleaseLockRequest,
    UpdateLockRequest,
)
from ._logger import get_logger
from ._ttl_dict import TTLDict
from .exception import (
    BucketNotFoundError,
    GCSApiError,
    LockConflictError,
    LockNotHeldError,
)


@dataclass(frozen=True)
class LockState:
    """
    Immutable representation of an acquired lock.

    Acts as a context manager that automatically releases the lock on exit
    if it is still held.

    Attributes:
        bucket: The GCS bucket name where the lock object resides.
        lock_id: The object key identifying the lock.
        lock_owner: Identifier of the owner (client) holding the lock.
        expires_at: UTC timestamp when the lock will expire on GCS.
        _gcs_lock: Back-reference to the GcsLock instance that acquired it.
    """

    bucket: str
    lock_id: str
    lock_owner: str
    expires_at: datetime
    _gcs_lock: "GcsLock"

    def __enter__(self):
        """
        Enter the lock context.

        Returns:
            LockState: This instance, for use within a with-statement.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the lock context, releasing the lock.

        The lock is released via the owning GcsLock instance. If releasing
        fails with a GCSApiError, a warning is logged and the error is re-raised.
        If an exception occurred within the context and releasing fails,
        the original exception is re-raised.

        Args:
            exc_type: Exception type raised within the context, if any.
            exc_val: Exception instance raised within the context, if any.
            exc_tb: Traceback for the exception raised within the context, if any.

        Raises:
            GCSApiError: If releasing the lock fails and no prior exception
                should take precedence.
        """
        try:
            self._gcs_lock.release(self)
        except GCSApiError as e:
            get_logger().warn(
                f"Failed to release lock {self.lock_id} due to {e}",
            )
            if exc_type:
                raise exc_val
            raise e

        if exc_type:
            raise exc_val

    def is_expired(self) -> bool:
        """
        Check whether the lock has already expired.

        Returns:
            bool: True if current UTC time is past expires_at; otherwise False.
        """
        return self.expires_at < datetime.now(timezone.utc)

    @property
    def lock_state_id(self):
        """
        A stable identifier for this lock state, combining bucket and lock_id.

        Returns:
            str: The identifier in the form "<bucket>:<lock_id>".
        """
        return f"{self.bucket}:{self.lock_id}"


class GcsLock:
    """
    Client for acquiring and releasing cooperative locks stored in GCS.

    Each lock is represented by a GCS object and guarded via metadata
    and conditional requests to avoid conflicts between owners.
    """

    __slots__ = ("_bucket", "_locked_owner", "_accessor", "_manage_lock_state_table")

    def __init__(
        self,
        bucket_name: str,
        lock_owner: str | None = None,
        credentials: Credentials | None = None,
    ):
        """
        Initialize a GcsLock client.

        Args:
            bucket_name: Target GCS bucket where lock objects are stored.
            lock_owner: Optional owner identifier. If not provided, a random UUID
                is generated to represent this client instance.
            credentials: Optional Google auth credentials. If not provided,
                application default credentials are used.

        Notes:
            The owner ID is used to determine whether an existing lock may be
            refreshed/updated by this client.
        """
        if lock_owner is None:
            lock_owner = str(uuid.uuid4())

        self._bucket = bucket_name
        self._locked_owner = lock_owner
        self._accessor: Accessor = RestAccessor(credentials)
        self._manage_lock_state_table: TTLDict[str, LockResponse] = TTLDict()

    def acquire(self, lock_id: str, expires_seconds: int = 30):
        """
        Acquire or refresh a lock for the given object key.

        If the lock does not yet exist, it is created. If it exists and is
        owned by this client (or otherwise available), its expiration is updated.
        In case of a conflict (owned by another active owner), an error is raised.

        Args:
            lock_id: The object key identifying the lock in the bucket.
            expires_seconds: Time-to-live (in seconds) for the lock from now.

        Returns:
            LockState: An immutable handle representing the acquired lock.

        Raises:
            BucketNotFoundError: If the configured bucket does not exist.
            LockConflictError: If the lock is currently owned by another owner
                and cannot be acquired.
            GCSApiError: On underlying GCS API failures.
        """
        self._ensure_bucket_exists()

        response = self._resolve_lock_response(lock_id, expires_seconds)

        lock_state = LockState(
            bucket=self._bucket,
            lock_id=lock_id,
            lock_owner=self._locked_owner,
            expires_at=response.expires_at,
            _gcs_lock=self,
        )

        lock_state_id = lock_state.lock_state_id
        self._manage_lock_state_table[lock_state_id] = response, response.expires_sec

        return lock_state

    def release(self, lock_state: LockState):
        """
        Release a previously acquired lock.

        The lock must have been acquired by this GcsLock instance. If the lock
        has already expired or was not tracked, a LockNotHeldError is raised.

        Args:
            lock_state: The lock state to release.

        Raises:
            LockNotHeldError: If the lock_state was not acquired or already released
                by this instance.
            LockConflictError: If the underlying lock has changed externally and
                cannot be released with the known generation.
            GCSApiError: On underlying GCS API failures.
        """
        internal_id = lock_state.lock_state_id
        try:
            lock_instance = self._manage_lock_state_table[internal_id]
        except KeyError:
            raise LockNotHeldError(lock_state.lock_state_id)

        release_request = ReleaseLockRequest(
            bucket=self._bucket,
            object_key=lock_state.lock_id,
            generation=lock_instance.generation,
            metageneration=lock_instance.metageneration,
        )

        self._accessor.release_lock(release_request)

        del self._manage_lock_state_table[internal_id]

    def _ensure_bucket_exists(self):
        """
        Verify that the target bucket exists.

        Raises:
            BucketNotFoundError: If the bucket does not exist or is inaccessible.
            GCSApiError: On underlying GCS API failures when checking existence.
        """
        bucket_exists = BucketExistsRequest(bucket=self._bucket)
        if not self._accessor.bucket_exists(bucket_exists):
            raise BucketNotFoundError(self._bucket)

    def _resolve_lock_response(
        self, lock_id: str, expires_seconds: int
    ) -> LockResponse:
        """
        Create or update the lock on GCS and return the current lock response.

        If no lock exists, it is created. If a lock exists and can be acquired
        by this owner, it is updated (refreshed). Otherwise a conflict is raised.

        Args:
            lock_id: The object key identifying the lock.
            expires_seconds: Desired TTL for the lock from now.

        Returns:
            LockResponse: The response describing the lock state on GCS.

        Raises:
            LockConflictError: If the lock is currently owned by another owner
                and cannot be updated by this client.
            GCSApiError: On underlying GCS API failures.
        """
        info_req = GetLockInfoRequest(bucket=self._bucket, object_key=lock_id)
        lock_info = self._accessor.get_lock_info(info_req)

        if lock_info is None:  # first time acquiring lock
            req = AcquireLockRequest(
                bucket=self._bucket,
                object_key=lock_id,
                expires_sec=expires_seconds,
                owner=self._locked_owner,
            )
            return self._accessor.acquire_lock(req)
        elif lock_info.can_be_acquire_by(self._locked_owner):
            req = UpdateLockRequest(
                bucket=self._bucket,
                object_key=lock_id,
                metageneration=lock_info.metageneration,
                expires_sec=expires_seconds,
                owner=self._locked_owner,
            )
            return self._accessor.update_lock(req)
        else:
            raise LockConflictError(self._bucket, lock_id)
