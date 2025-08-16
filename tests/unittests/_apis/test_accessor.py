import json
import types
from datetime import datetime, timezone

import pytest

from gcslock._apis import accessor as target
from gcslock._apis.model import (
    AcquireLockRequest,
    BucketExistsRequest,
    GetLockInfoRequest,
    LockResponse,
    ReleaseLockRequest,
    UpdateLockRequest,
)
from gcslock.exception import LockConflictError, UnexpectedGCSResponseError


class DummyResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body or {}
        self.text = json.dumps(self._body)
        self.request = types.SimpleNamespace(body=None)

    def json(self):
        return self._body


@pytest.fixture
def sample_lock_body():
    return {
        "bucket": "bkt",
        "name": "obj",
        "generation": 1,
        "metageneration": 2,
        "updated": "2025-01-01T00:00:00Z",
        "metadata": {"expires_sec": "10", "lock_owner": "alice"},
    }


class TestResponseToLockInfo:
    def test_parses_fields_normal(self, sample_lock_body):
        resp = DummyResponse(body=sample_lock_body)
        lr = target._response_to_lock_info(resp)
        assert isinstance(lr, LockResponse)
        assert lr.bucket == "bkt"
        assert lr.lock_owner == "alice"
        assert lr.expires_sec == 10

    def test_expires_sec_non_positive(self, sample_lock_body):
        body2 = dict(sample_lock_body)
        body2["metadata"] = {"expires_sec": "-5", "lock_owner": "bob"}
        lr = target._response_to_lock_info(DummyResponse(body=body2))
        assert lr.expires_sec == 0


class TestResponseFields:
    def test_contains_expected_keys(self):
        fields = target._response_fields()
        expected = {
            "metadata",
            "generation",
            "updated",
            "metageneration",
            "bucket",
            "name",
        }
        assert expected <= fields


@pytest.fixture
def rest_accessor(monkeypatch):
    class DummySession:
        def __init__(self, **kwargs):
            self._calls = []

        def get(self, *a, **k):
            return self._calls.pop(0)

        def post(self, *a, **k):
            return self._calls.pop(0)

        def patch(self, *a, **k):
            return self._calls.pop(0)

        def delete(self, *a, **k):
            return self._calls.pop(0)

    monkeypatch.setattr(
        target, "AuthorizedSession", lambda credentials=None: DummySession()
    )
    return target.RestAccessor(
        credentials=None,
        logger=types.SimpleNamespace(
            debug=lambda *a, **k: None, warning=lambda *a, **k: None
        ),
    )


class TestBucketExists:
    def test_bucket_exists_true(self, rest_accessor):
        rest_accessor._authed_session._calls.append(
            DummyResponse(status_code=200, body={"name": "bkt"})
        )
        assert rest_accessor.bucket_exists(BucketExistsRequest(bucket="b"))

    def test_bucket_exists_false(self, rest_accessor):
        rest_accessor._authed_session._calls.append(DummyResponse(status_code=404))
        assert rest_accessor.bucket_exists(BucketExistsRequest(bucket="b")) is False

    def test_bucket_exists_unexpected_status(self, rest_accessor):
        rest_accessor._authed_session._calls.append(DummyResponse(status_code=500))
        with pytest.raises(UnexpectedGCSResponseError):
            rest_accessor.bucket_exists(BucketExistsRequest(bucket="b"))


class TestGetLockInfo:
    def test_get_lock_info_none(self, rest_accessor):
        rest_accessor._authed_session._calls.append(DummyResponse(status_code=404))
        assert (
            rest_accessor.get_lock_info(GetLockInfoRequest(bucket="b", object_key="o"))
            is None
        )

    def test_get_lock_info_success(self, rest_accessor, sample_lock_body):
        rest_accessor._authed_session._calls.append(
            DummyResponse(status_code=200, body=sample_lock_body)
        )
        lr = rest_accessor.get_lock_info(GetLockInfoRequest(bucket="b", object_key="o"))
        assert isinstance(lr, LockResponse)

    def test_get_lock_info_unexpected(self, rest_accessor):
        rest_accessor._authed_session._calls.append(DummyResponse(status_code=418))
        with pytest.raises(UnexpectedGCSResponseError):
            rest_accessor.get_lock_info(GetLockInfoRequest(bucket="b", object_key="o"))


class TestAcquireLock:
    def test_acquire_lock_success(self, rest_accessor, sample_lock_body):
        rest_accessor._authed_session._calls.append(
            DummyResponse(status_code=200, body=sample_lock_body)
        )
        req = AcquireLockRequest(bucket="b", object_key="o", owner="alice")
        assert isinstance(rest_accessor.acquire_lock(req), LockResponse)

    def test_acquire_lock_conflict(self, rest_accessor):
        rest_accessor._authed_session._calls.append(DummyResponse(status_code=412))
        req = AcquireLockRequest(bucket="b", object_key="o", owner="alice")
        with pytest.raises(LockConflictError):
            rest_accessor.acquire_lock(req)

    def test_acquire_lock_unexpected(self, rest_accessor):
        rest_accessor._authed_session._calls.append(DummyResponse(status_code=500))
        req = AcquireLockRequest(bucket="b", object_key="o", owner="alice")
        with pytest.raises(UnexpectedGCSResponseError):
            rest_accessor.acquire_lock(req)

    def test_acquire_lock_force_true(self, rest_accessor, sample_lock_body):
        rest_accessor._authed_session._calls.append(
            DummyResponse(status_code=200, body=sample_lock_body)
        )
        req = AcquireLockRequest(bucket="b", object_key="o", owner="a", force=True)
        rest_accessor.acquire_lock(req)


class TestUpdateLock:
    def test_update_lock_success(self, rest_accessor, sample_lock_body):
        rest_accessor._authed_session._calls.append(
            DummyResponse(status_code=200, body=sample_lock_body)
        )
        req = UpdateLockRequest(
            bucket="b", object_key="o", metageneration=1, owner="alice"
        )
        assert isinstance(rest_accessor.update_lock(req), LockResponse)

    def test_update_lock_conflict(self, rest_accessor):
        rest_accessor._authed_session._calls.append(DummyResponse(status_code=412))
        req = UpdateLockRequest(
            bucket="b", object_key="o", metageneration=1, owner="alice"
        )
        with pytest.raises(LockConflictError):
            rest_accessor.update_lock(req)

    def test_update_lock_unexpected(self, rest_accessor):
        rest_accessor._authed_session._calls.append(DummyResponse(status_code=500))
        req = UpdateLockRequest(
            bucket="b", object_key="o", metageneration=1, owner="alice"
        )
        with pytest.raises(UnexpectedGCSResponseError):
            rest_accessor.update_lock(req)

    def test_update_lock_force_true(self, rest_accessor, sample_lock_body):
        rest_accessor._authed_session._calls.append(
            DummyResponse(status_code=200, body=sample_lock_body)
        )
        req = UpdateLockRequest(
            bucket="b", object_key="o", metageneration=1, owner="a", force=True
        )
        rest_accessor.update_lock(req)


class TestReleaseLock:
    def test_release_lock_success(self, rest_accessor):
        rest_accessor._authed_session._calls.append(DummyResponse(status_code=204))
        req = ReleaseLockRequest(
            bucket="b", object_key="o", generation=1, metageneration=1
        )
        assert rest_accessor.release_lock(req) is None

    def test_release_lock_warn_404(self, rest_accessor):
        called = {}
        rest_accessor._logger.warning = lambda *a, **k: called.setdefault(
            "warned", True
        )
        rest_accessor._authed_session._calls.append(DummyResponse(status_code=404))
        req = ReleaseLockRequest(
            bucket="b", object_key="o", generation=1, metageneration=1
        )
        rest_accessor.release_lock(req)
        assert called.get("warned")

    def test_release_lock_warn_412(self, rest_accessor):
        called = {}
        rest_accessor._logger.warning = lambda *a, **k: called.setdefault(
            "warned", True
        )
        rest_accessor._authed_session._calls.append(DummyResponse(status_code=412))
        req = ReleaseLockRequest(
            bucket="b", object_key="o", generation=1, metageneration=1
        )
        rest_accessor.release_lock(req)
        assert called.get("warned")

    def test_release_lock_unexpected(self, rest_accessor):
        rest_accessor._authed_session._calls.append(DummyResponse(status_code=500))
        req = ReleaseLockRequest(
            bucket="b", object_key="o", generation=1, metageneration=1
        )
        with pytest.raises(UnexpectedGCSResponseError):
            rest_accessor.release_lock(req)
