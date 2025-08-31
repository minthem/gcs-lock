import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from time import sleep

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

    __slots__ = ("_locked_owner", "_accessor", "_manage_lock_state_table")

    def __init__(
        self,
        lock_owner: str | None = None,
        credentials: Credentials | None = None,
    ):
        """
        Initialize a GcsLock client.

        Args:
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

        self._locked_owner = lock_owner
        self._accessor: Accessor = RestAccessor(credentials)
        self._manage_lock_state_table: TTLDict[str, LockResponse] = TTLDict()

    def acquire(
        self,
        bucket: str,
        object_key: str,
        expires_seconds: int = 30,
        *,
        max_wait_seconds: int = 0,
    ) -> LockState:
        """
        Acquire or refresh a lock for the given object key.

        If the lock does not yet exist, it is created. If it exists and is
        owned by this client (or otherwise available), its expiration is updated.
        In case of a conflict (owned by another active owner), an error is raised.

        Args:
            bucket: The name of the bucket where the lock object resides.
            object_key: The object key identifying the lock.
            expires_seconds: Time-to-live (in seconds) for the lock from now.
            max_wait_seconds: Maximum time to wait for the lock to become available.

        Returns:
            LockState: An immutable handle representing the acquired lock.

        Raises:
            ValueError: If max_wait_seconds is negative or expires_seconds is not positive.
            BucketNotFoundError: If the configured bucket does not exist.
            LockConflictError: If the lock is currently owned by another owner
                and cannot be acquired.
            GCSApiError: On underlying GCS API failures.
        """

        if max_wait_seconds < 0:
            raise ValueError("max_wait_seconds must be a non-negative integer")

        if expires_seconds < 1:
            raise ValueError("expires_seconds must be a positive integer")

        self._ensure_bucket_exists(bucket)
        wait_threshold = datetime.now(timezone.utc) + timedelta(
            seconds=max_wait_seconds
        )

        while True:
            try:
                response = self._resolve_lock_response(
                    bucket, object_key, expires_seconds
                )
                break
            except LockConflictError:
                release_time = self._get_release_time(bucket, object_key)
                if wait_threshold <= release_time:
                    raise

                current_time = datetime.now(timezone.utc)
                wait_seconds = (
                    min(release_time, wait_threshold) - current_time
                ).total_seconds()
                get_logger().debug(
                    f"Lock gs://{bucket}/{object_key} is currently held by another owner. "
                    f"Waiting for {wait_seconds} seconds before retrying..."
                )
                sleep(wait_seconds if 0 < wait_seconds else 0)

        lock_state = LockState(
            bucket=bucket,
            lock_id=object_key,
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
            bucket=lock_state.bucket,
            object_key=lock_state.lock_id,
            generation=lock_instance.generation,
            metageneration=lock_instance.metageneration,
        )

        self._accessor.release_lock(release_request)

        del self._manage_lock_state_table[internal_id]

    def _ensure_bucket_exists(self, bucket):
        """
        Verify that the target bucket exists.

        Raises:
            BucketNotFoundError: If the bucket does not exist or is inaccessible.
            GCSApiError: On underlying GCS API failures when checking existence.
        """
        bucket_exists = BucketExistsRequest(bucket)
        if not self._accessor.bucket_exists(bucket_exists):
            raise BucketNotFoundError(bucket)

    def _resolve_lock_response(
        self, bucket: str, object_key: str, expires_seconds: int
    ) -> LockResponse:
        """
        Create or update the lock on GCS and return the current lock response.

        If no lock exists, it is created. If a lock exists and can be acquired
        by this owner, it is updated (refreshed). Otherwise a conflict is raised.

        Args:
            bucket: The name of the bucket where the lock object resides.
            object_key: The object key identifying the lock.
            expires_seconds: Desired TTL for the lock from now.

        Returns:
            LockResponse: The response describing the lock state on GCS.

        Raises:
            LockConflictError: If the lock is currently owned by another owner
                and cannot be updated by this client.
            GCSApiError: On underlying GCS API failures.
        """
        info_req = GetLockInfoRequest(bucket=bucket, object_key=object_key)
        lock_info = self._accessor.get_lock_info(info_req)

        if lock_info is None:  # first time acquiring lock
            req = AcquireLockRequest(
                bucket=bucket,
                object_key=object_key,
                expires_sec=expires_seconds,
                owner=self._locked_owner,
            )
            return self._accessor.acquire_lock(req)
        elif lock_info.can_be_acquire_by(self._locked_owner):
            req = UpdateLockRequest(
                bucket=bucket,
                object_key=object_key,
                metageneration=lock_info.metageneration,
                expires_sec=expires_seconds,
                owner=self._locked_owner,
            )
            return self._accessor.update_lock(req)
        else:
            raise LockConflictError(bucket, object_key)

    def _get_release_time(self, bucket: str, object_key: str) -> datetime:
        """
        Retrieves the release time of a lock for the specified object in a bucket.

        The method fetches lock information for an object key in a specific bucket
        using the underlying accessor. If no lock information is available, it
        returns the current time in UTC. Otherwise, it extracts and returns the
        lock's expiration time.

        Args:
            bucket: The name of the bucket where the object resides.
            object_key: The key identifying the specific object in the bucket.

        Returns:
            The lock release/expiration time of the specified object.
        """
        info_req = GetLockInfoRequest(bucket=bucket, object_key=object_key)
        lock_info = self._accessor.get_lock_info(info_req)

        if lock_info is None:
            return datetime.now(timezone.utc)

        return lock_info.expires_at
