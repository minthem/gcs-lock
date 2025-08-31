import json
import uuid
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from gcslock.core import (
    GcsLock,
    LockResponse,
    LockState,
    ReleaseLockRequest,
)
from gcslock.exception import (
    BucketNotFoundError,
    GCSApiError,
    GcsClientError,
    LockConflictError,
    LockNotHeldError,
    UnexpectedGCSResponseError,
)


@pytest.mark.unittest
class TestGcsLock:
    BUCKET = "example"
    LOCK_ID = "test"
    FIXED_UUID = uuid.UUID("12345678-1234-1234-1234-123456789012")

    @pytest.fixture
    def env(self, monkeypatch) -> SimpleNamespace:
        # uuid4 → 固定値
        monkeypatch.setattr("gcslock.core.uuid.uuid4", lambda: self.FIXED_UUID)

        # RestAccessor → 単一の MagicMock を返すファクトリ
        accessor = MagicMock()
        accessor.bucket_exists.return_value = True

        def accessor_factory(credentials=None, **kwargs):
            accessor._credentials = credentials
            accessor._kwargs = kwargs
            return accessor

        monkeypatch.setattr("gcslock.core.RestAccessor", accessor_factory)

        return SimpleNamespace(accessor=accessor)

    @pytest.fixture(scope="class")
    def error_response_401(self):
        return GcsClientError(
            status_code=401,
            message="Login Required",
            details={
                "errors": [
                    {
                        "domain": "global",
                        "reason": "required",
                        "message": "Login Required",
                        "locationType": "header",
                        "location": "Authorization",
                    }
                ],
            },
        )

    @pytest.fixture(scope="class")
    def error_response_500(self):
        return UnexpectedGCSResponseError(
            status_code=500,
            response="Internal Server Error",
        )

    @staticmethod
    def _lr(
        bucket: str,
        key: str,
        owner: str,
        *,
        locked_at: datetime,
        expires_sec: int,
        generation: int = 1234567890,
        metageneration: int = 1,
    ) -> LockResponse:
        return LockResponse(
            bucket=bucket,
            object_key=key,
            generation=generation,
            metageneration=metageneration,
            lock_owner=owner,
            locked_at=locked_at,
            expires_sec=expires_sec,
        )

    def test_acquire_lock_new(self, env):
        bucket = self.BUCKET
        lock_id = self.LOCK_ID
        expires_sec = 10

        lock_obj = GcsLock(bucket_name=bucket)
        env.accessor.get_lock_info.return_value = None

        lr = self._lr(
            bucket=bucket,
            key=lock_id,
            owner=str(self.FIXED_UUID),
            locked_at=datetime.now(timezone.utc),
            expires_sec=expires_sec,
        )
        env.accessor.acquire_lock.return_value = lr

        lock_state = lock_obj.acquire(lock_id=lock_id, expires_seconds=expires_sec)

        assert lock_state.bucket == bucket
        assert lock_state.lock_id == lock_id
        assert lock_state.lock_owner == str(self.FIXED_UUID)
        assert lock_state.expires_at == lr.expires_at
        assert lock_state._gcs_lock == lock_obj
        assert lock_state.lock_state_id == f"{bucket}:{lock_id}"

    def test_acquire_lock_update(self, env):
        bucket = self.BUCKET
        lock_id = self.LOCK_ID
        expires_sec = 20

        # 既存のロックは同一オーナーで期限切れ
        old_lr = self._lr(
            bucket=bucket,
            key=lock_id,
            owner=str(self.FIXED_UUID),
            locked_at=datetime.now(timezone.utc) - timedelta(seconds=expires_sec + 1),
            expires_sec=expires_sec,
            generation=1,
            metageneration=1,
        )
        env.accessor.get_lock_info.return_value = old_lr

        new_lr = self._lr(
            bucket=bucket,
            key=lock_id,
            owner=str(self.FIXED_UUID),
            locked_at=datetime.now(timezone.utc),
            expires_sec=expires_sec,
        )
        env.accessor.update_lock.return_value = new_lr

        lock_obj = GcsLock(bucket_name=bucket)
        lock_state = lock_obj.acquire(lock_id=lock_id, expires_seconds=expires_sec)

        assert lock_state.bucket == bucket
        assert lock_state.lock_id == lock_id
        assert lock_state.lock_owner == str(self.FIXED_UUID)
        assert lock_state.expires_at == new_lr.expires_at
        assert lock_state._gcs_lock == lock_obj
        assert lock_state.lock_state_id == f"{bucket}:{lock_id}"

    def test_release(self, env):
        bucket = self.BUCKET
        lock_id = self.LOCK_ID
        expires_sec = 10

        env.accessor.get_lock_info.return_value = None
        lr = self._lr(
            bucket=bucket,
            key=lock_id,
            owner=str(self.FIXED_UUID),
            locked_at=datetime.now(timezone.utc),
            expires_sec=expires_sec,
        )
        env.accessor.acquire_lock.return_value = lr

        lock_obj = GcsLock(bucket_name=bucket)
        lock_state = lock_obj.acquire(lock_id=lock_id, expires_seconds=expires_sec)
        lock_obj.release(lock_state)

        rr = ReleaseLockRequest(
            bucket=bucket,
            object_key=lock_id,
            generation=lr.generation,
            metageneration=lr.metageneration,
        )
        env.accessor.release_lock.assert_called_once_with(rr)
        assert lock_state.lock_state_id not in lock_obj._manage_lock_state_table

    def test_acquire_lock_conflict(self, env):
        bucket = self.BUCKET
        lock_id = self.LOCK_ID
        expires_sec = 1

        # 他オーナーが保持中 → 競合
        current_lr = self._lr(
            bucket=bucket,
            key=lock_id,
            owner="other-owner",
            locked_at=datetime.now(timezone.utc),
            expires_sec=expires_sec,
            generation=1,
            metageneration=1,
        )
        env.accessor.get_lock_info.return_value = current_lr

        lock_obj = GcsLock(bucket_name=bucket)
        with pytest.raises(LockConflictError):
            lock_obj.acquire(lock_id=lock_id, expires_seconds=expires_sec)

    def test_acquire_lock_conflict_wait(self, env):
        bucket = self.BUCKET
        lock_id = self.LOCK_ID
        expires_sec = 2

        current_lr = self._lr(
            bucket=bucket,
            key=lock_id,
            owner="other-owner",
            locked_at=datetime.now(timezone.utc),
            expires_sec=expires_sec,
            generation=1,
            metageneration=1,
        )
        env.accessor.get_lock_info.return_value = current_lr

        lock_obj = GcsLock(bucket_name=bucket)
        lock_state = lock_obj.acquire(
            lock_id=lock_id, expires_seconds=expires_sec, max_wait_seconds=10
        )

        assert lock_state.bucket == bucket
        assert lock_state.lock_id == lock_id
        assert lock_state.lock_owner == str(self.FIXED_UUID)
        assert lock_state._gcs_lock == lock_obj
        assert lock_state.lock_state_id == f"{bucket}:{lock_id}"

    def test_acquire_lock_conflict_wait_timeout(self, env):
        bucket = self.BUCKET
        lock_id = self.LOCK_ID
        expires_sec = 10

        current_lr = self._lr(
            bucket=bucket,
            key=lock_id,
            owner="other-owner",
            locked_at=datetime.now(timezone.utc),
            expires_sec=expires_sec,
            generation=1,
            metageneration=1,
        )
        env.accessor.get_lock_info.return_value = current_lr

        lock_obj = GcsLock(bucket_name=bucket)
        with pytest.raises(LockConflictError):
            lock_obj.acquire(
                lock_id=lock_id, expires_seconds=expires_sec, max_wait_seconds=5
            )

    def test_acquire_lock_conflict_wait_no_lock(self, env):
        bucket = self.BUCKET
        lock_id = self.LOCK_ID
        expires_sec = 1

        current_lr = self._lr(
            bucket=bucket,
            key=lock_id,
            owner="other-owner",
            locked_at=datetime.now(timezone.utc),
            expires_sec=expires_sec,
            generation=1,
            metageneration=1,
        )
        env.accessor.get_lock_info.side_effect = [current_lr, None, None]

        lock_obj = GcsLock(bucket_name=bucket)
        lock_state = lock_obj.acquire(
            lock_id=lock_id, expires_seconds=expires_sec, max_wait_seconds=5
        )

        assert lock_state.bucket == bucket
        assert lock_state.lock_id == lock_id
        assert lock_state.lock_owner == str(self.FIXED_UUID)
        assert lock_state._gcs_lock == lock_obj
        assert lock_state.lock_state_id == f"{bucket}:{lock_id}"

    def test_bucket_exists_not_found(self, env):
        env.accessor.bucket_exists.return_value = False
        lock_obj = GcsLock(bucket_name=self.BUCKET)
        with pytest.raises(BucketNotFoundError):
            lock_obj.acquire(lock_id=self.LOCK_ID)

    def test_bucket_exists_raise_error(self, env, error_response_401):
        env.accessor.bucket_exists.side_effect = error_response_401
        lock_obj = GcsLock(bucket_name=self.BUCKET)
        with pytest.raises(GcsClientError):
            lock_obj.acquire(lock_id=self.LOCK_ID)

    def test_acquire_lock_new_lock_raise_client_error(self, env, error_response_401):
        env.accessor.get_lock_info.return_value = None
        env.accessor.acquire_lock.side_effect = error_response_401

        lock_obj = GcsLock(bucket_name=self.BUCKET)
        with pytest.raises(GcsClientError):
            lock_obj.acquire(lock_id=self.LOCK_ID, expires_seconds=10)

    def test_acquire_lock_new_lock_raise_server_error(self, env, error_response_500):
        env.accessor.get_lock_info.return_value = None
        env.accessor.acquire_lock.side_effect = error_response_500

        lock_obj = GcsLock(bucket_name=self.BUCKET)
        with pytest.raises(UnexpectedGCSResponseError):
            lock_obj.acquire(lock_id=self.LOCK_ID, expires_seconds=10)

    def test_acquire_lock_update_raise_client_error(self, env, error_response_401):
        bucket = self.BUCKET
        lock_id = self.LOCK_ID
        expires_sec = 20

        old_lr = self._lr(
            bucket=bucket,
            key=lock_id,
            owner=str(self.FIXED_UUID),
            locked_at=datetime.now(timezone.utc) - timedelta(seconds=expires_sec + 1),
            expires_sec=expires_sec,
            generation=1,
            metageneration=1,
        )
        env.accessor.get_lock_info.return_value = old_lr
        env.accessor.update_lock.side_effect = error_response_401

        lock_obj = GcsLock(bucket_name=bucket)
        with pytest.raises(GcsClientError):
            lock_obj.acquire(lock_id=lock_id, expires_seconds=expires_sec)

    def test_acquire_lock_update_raise_error(self, env, error_response_500):
        bucket = self.BUCKET
        lock_id = self.LOCK_ID
        expires_sec = 20

        old_lr = self._lr(
            bucket=bucket,
            key=lock_id,
            owner=str(self.FIXED_UUID),
            locked_at=datetime.now(timezone.utc) - timedelta(seconds=expires_sec + 1),
            expires_sec=expires_sec,
            generation=1,
            metageneration=1,
        )
        env.accessor.get_lock_info.return_value = old_lr
        env.accessor.update_lock.side_effect = error_response_500

        lock_obj = GcsLock(bucket_name=bucket)
        with pytest.raises(UnexpectedGCSResponseError):
            lock_obj.acquire(lock_id=lock_id, expires_seconds=expires_sec)

    def test_acquire_lock_invalid_expires_sec(self, env):
        expires_sec = 0
        lock_obj = GcsLock(bucket_name=self.BUCKET)
        with pytest.raises(ValueError):
            lock_obj.acquire(lock_id=self.LOCK_ID, expires_seconds=expires_sec)

    def test_acquire_lock_invalid_wait_seconds(self, env):
        max_wait_sec = -1
        lock_obj = GcsLock(bucket_name=self.BUCKET)
        with pytest.raises(ValueError):
            lock_obj.acquire(lock_id=self.LOCK_ID, max_wait_seconds=max_wait_sec)

    def test_release_raise_not_held_after_local_eviction(self, env):
        expires_sec = 5
        env.accessor.get_lock_info.return_value = None
        lr = self._lr(
            bucket=self.BUCKET,
            key=self.LOCK_ID,
            owner=str(self.FIXED_UUID),
            locked_at=datetime.now(timezone.utc),
            expires_sec=expires_sec,
        )
        env.accessor.acquire_lock.return_value = lr

        lock_obj = GcsLock(bucket_name=self.BUCKET)
        lock_state = lock_obj.acquire(lock_id=self.LOCK_ID, expires_seconds=expires_sec)

        # 管理テーブルから手動で削除して「このインスタンスで保持していない」状態にする
        del lock_obj._manage_lock_state_table[lock_state.lock_state_id]

        with pytest.raises(
            LockNotHeldError,
            match=f"Lock '{lock_state.lock_state_id}' was not acquired or already released by this instance",
        ):
            lock_obj.release(lock_state)

    def test_release_raise_not_exists_key(self, env):
        expires_sec = 5
        env.accessor.get_lock_info.return_value = None
        lr = self._lr(
            bucket=self.BUCKET,
            key=self.LOCK_ID,
            owner=str(self.FIXED_UUID),
            locked_at=datetime.now(timezone.utc),
            expires_sec=expires_sec,
        )
        env.accessor.acquire_lock.return_value = lr

        lock_obj = GcsLock(bucket_name=self.BUCKET)
        _ = lock_obj.acquire(lock_id=self.LOCK_ID, expires_seconds=expires_sec)

        invalid_state = LockState(
            bucket=self.BUCKET,
            lock_id="invalid",
            lock_owner=str(uuid.uuid4()),
            expires_at=lr.expires_at,
            _gcs_lock=lock_obj,
        )

        with pytest.raises(
            LockNotHeldError,
            match=f"Lock '{invalid_state.lock_state_id}' was not acquired or already released by this instance",
        ):
            lock_obj.release(invalid_state)


@pytest.mark.unittest
class TestLockState:
    def _make(self, mock_gcs_lock=None, expires_at=None):
        if mock_gcs_lock is None:
            mock_gcs_lock = MagicMock()
        if expires_at is None:
            expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)

        return LockState(
            bucket="test-bucket",
            lock_id="lock-123",
            lock_owner="owner",
            expires_at=expires_at,
            _gcs_lock=mock_gcs_lock,
        )

    def test_enter_returns_self(self):
        ls = self._make()
        assert ls.__enter__() is ls

    def test_lock_state_id(self):
        ls = self._make()
        assert ls.lock_state_id == "test-bucket:lock-123"

    def test_is_expired_false_when_future(self):
        ls = self._make(expires_at=datetime.now(timezone.utc) + timedelta(seconds=1))
        assert ls.is_expired() is False

    def test_is_expired_true_when_past(self):
        ls = self._make(expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        assert ls.is_expired() is True

    def test_frozen_dataclass_immutable(self):
        ls = self._make()
        with pytest.raises(FrozenInstanceError):
            ls.bucket = "other"

    def test_exit_normal_calls_release_and_no_log(self, monkeypatch):
        logger = MagicMock()
        monkeypatch.setattr("gcslock.core.get_logger", lambda: logger)

        mock_lock = MagicMock()
        ls = self._make(mock_gcs_lock=mock_lock)

        result = ls.__exit__(None, None, None)
        assert result is None
        mock_lock.release.assert_called_once_with(ls)
        logger.warn.assert_not_called()

    def test_exit_with_inner_exception_reraises_original(self, monkeypatch):
        logger = MagicMock()
        monkeypatch.setattr("gcslock.core.get_logger", lambda: logger)

        mock_lock = MagicMock()
        ls = self._make(mock_gcs_lock=mock_lock)

        class InnerErr(Exception):
            pass

        with pytest.raises(InnerErr):
            ls.__exit__(InnerErr, InnerErr("boom"), None)

        mock_lock.release.assert_called_once_with(ls)
        logger.warn.assert_not_called()

    def test_exit_release_raises_gcsapierror_without_inner_reraises_gcs(
        self, monkeypatch
    ):
        logger = MagicMock()
        monkeypatch.setattr("gcslock.core.get_logger", lambda: logger)

        mock_lock = MagicMock()
        mock_lock.release.side_effect = UnexpectedGCSResponseError(
            status_code=500, response="gcs fail"
        )
        ls = self._make(mock_gcs_lock=mock_lock)

        with pytest.raises(GCSApiError):
            ls.__exit__(None, None, None)

        mock_lock.release.assert_called_once_with(ls)
        logger.warn.assert_called_once()

    def test_exit_release_raises_gcsapierror_with_inner_reraises_inner(
        self, monkeypatch
    ):
        logger = MagicMock()
        monkeypatch.setattr("gcslock.core.get_logger", lambda: logger)

        mock_lock = MagicMock()
        mock_lock.release.side_effect = UnexpectedGCSResponseError(
            status_code=500, response="gcs fail"
        )
        ls = self._make(mock_gcs_lock=mock_lock)

        class InnerErr(Exception):
            pass

        with pytest.raises(InnerErr):
            ls.__exit__(InnerErr, InnerErr("inner"), None)

        mock_lock.release.assert_called_once_with(ls)
        logger.warn.assert_called_once()
