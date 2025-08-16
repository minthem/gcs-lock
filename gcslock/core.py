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
    bucket: str
    lock_id: str
    lock_owner: str
    expires_at: datetime
    _gcs_lock: "GcsLock"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
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
        return self.expires_at < datetime.now(timezone.utc)

    @property
    def lock_state_id(self):
        return f"{self.bucket}:{self.lock_id}"


class GcsLock:

    __slots__ = ("_bucket", "_locked_owner", "_accessor", "_manage_lock_state_table")

    def __init__(
        self,
        bucket_name: str,
        lock_owner: str | None = None,
        credentials: Credentials | None = None,
    ):
        if lock_owner is None:
            lock_owner = str(uuid.uuid4())

        if credentials is None:
            credentials, _ = default()

        self._bucket = bucket_name
        self._locked_owner = lock_owner
        self._accessor: Accessor = RestAccessor(credentials)
        self._manage_lock_state_table: TTLDict[str, LockResponse] = TTLDict()

    def acquire(self, lock_id: str, expires_seconds: int = 30):
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
        bucket_exists = BucketExistsRequest(bucket=self._bucket)
        if not self._accessor.bucket_exists(bucket_exists):
            raise BucketNotFoundError(self._bucket)

    def _resolve_lock_response(
        self, lock_id: str, expires_seconds: int
    ) -> LockResponse:
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
