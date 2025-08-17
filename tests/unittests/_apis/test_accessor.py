import json
import types

import pytest

from gcslock._apis import accessor as target
from gcslock.exception import (
    GcsClientError,
    LockConflictError,
    UnexpectedGCSResponseError,
)


class DummyResponse:
    def __init__(
        self, status_code: int, body: dict | None = None, text: str | None = None
    ):
        self.status_code = status_code
        self._body = body if body is not None else {}
        self.text = text if text is not None else json.dumps(self._body)
        self.request = types.SimpleNamespace(body=None)

    def json(self):
        return self._body


def sample_lock_body(
    owner="alice",
    expires_sec=30,
    generation=1,
    metageneration=1,
    bucket="b",
    name="locks/sample",
    updated="2024-01-01T00:00:00Z",
):
    return {
        "bucket": bucket,
        "name": name,
        "generation": generation,
        "metageneration": metageneration,
        "updated": updated,
        "metadata": {
            "lock_owner": owner,
            "expires_sec": str(expires_sec),
        },
    }


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


@pytest.mark.unittest
class TestResponseToLockInfo:
    def test_parses_fields_normal(self):
        body = sample_lock_body()
        res = DummyResponse(200, body)
        lock = target._response_to_lock_info(res)
        assert lock.lock_owner == "alice"
        assert lock.expires_sec == 30
        assert lock.generation == 1
        assert lock.metageneration == 1
        assert lock.object_key == "locks/sample"
        assert lock.bucket == "b"
        assert str(lock.locked_at.tzinfo) is not None

    def test_expires_sec_non_positive(self):
        body = sample_lock_body(expires_sec=0)
        res = DummyResponse(200, body)
        lock = target._response_to_lock_info(res)
        assert lock.expires_sec == 0


@pytest.mark.unittest
class TestResponseFields:
    def test_contains_expected_keys(self):
        fields = set(target._response_fields())
        for key in {
            "metadata",
            "generation",
            "updated",
            "metageneration",
            "bucket",
            "name",
        }:
            assert key in fields


@pytest.mark.unittest
class TestBucketExists:
    def test_bucket_exists_true(self, rest_accessor):
        rest_accessor._authed_session._calls.append(
            DummyResponse(200, {"name": "bucket"})
        )
        assert (
            rest_accessor.bucket_exists(target.BucketExistsRequest(bucket="bucket"))
            is True
        )

    def test_bucket_exists_false(self, rest_accessor):
        rest_accessor._authed_session._calls.append(
            DummyResponse(404, {"error": {"code": 404}})
        )
        assert (
            rest_accessor.bucket_exists(target.BucketExistsRequest(bucket="bucket"))
            is False
        )

    def test_bucket_exists_4xx_raises_client_error(self, rest_accessor):
        body = {
            "error": {
                "code": 400,
                "message": "Bad Request",
                "status": "INVALID_ARGUMENT",
            }
        }
        rest_accessor._authed_session._calls.append(DummyResponse(400, body))
        with pytest.raises(GcsClientError) as e:
            rest_accessor.bucket_exists(target.BucketExistsRequest(bucket="bucket"))
        assert e.value.status_code == 400

    def test_bucket_exists_5xx_raises_unexpected_error(self, rest_accessor):
        rest_accessor._authed_session._calls.append(
            DummyResponse(503, None, text="Service unavailable")
        )
        with pytest.raises(UnexpectedGCSResponseError) as e:
            rest_accessor.bucket_exists(target.BucketExistsRequest(bucket="bucket"))
        assert e.value.status_code == 503


@pytest.mark.unittest
class TestGetLockInfo:
    def test_get_lock_info_none(self, rest_accessor):
        rest_accessor._authed_session._calls.append(
            DummyResponse(404, {"error": {"code": 404}})
        )
        got = rest_accessor.get_lock_info(
            target.GetLockInfoRequest(bucket="b", object_key="k")
        )
        assert got is None

    def test_get_lock_info_success(self, rest_accessor):
        body = sample_lock_body(owner="alice", name="k")
        rest_accessor._authed_session._calls.append(DummyResponse(200, body))
        got = rest_accessor.get_lock_info(
            target.GetLockInfoRequest(bucket="b", object_key="k")
        )
        assert got is not None
        assert got.lock_owner == "alice"

    def test_get_lock_info_4xx(self, rest_accessor):
        body = {
            "error": {
                "code": 401,
                "message": "Unauthorized",
                "status": "UNAUTHENTICATED",
            }
        }
        rest_accessor._authed_session._calls.append(DummyResponse(401, body))
        with pytest.raises(GcsClientError) as e:
            rest_accessor.get_lock_info(
                target.GetLockInfoRequest(bucket="b", object_key="k")
            )
        assert e.value.status_code == 401

    def test_get_lock_info_5xx(self, rest_accessor):
        rest_accessor._authed_session._calls.append(
            DummyResponse(500, None, text="Internal error")
        )
        with pytest.raises(UnexpectedGCSResponseError) as e:
            rest_accessor.get_lock_info(
                target.GetLockInfoRequest(bucket="b", object_key="k")
            )
        assert e.value.status_code == 500


@pytest.mark.unittest
class TestAcquireLock:
    def test_acquire_lock_success(self, rest_accessor):
        body = sample_lock_body()
        rest_accessor._authed_session._calls.append(DummyResponse(200, body))
        got = rest_accessor.acquire_lock(
            target.AcquireLockRequest(
                bucket="b", object_key="k", expires_sec=30, owner="alice", force=False
            )
        )
        assert got.lock_owner == "alice"

    def test_acquire_lock_conflict(self, rest_accessor):
        rest_accessor._authed_session._calls.append(
            DummyResponse(412, {"error": {"code": 412}})
        )
        with pytest.raises(LockConflictError):
            rest_accessor.acquire_lock(
                target.AcquireLockRequest(
                    bucket="b",
                    object_key="k",
                    expires_sec=30,
                    owner="alice",
                    force=False,
                )
            )

    def test_acquire_lock_4xx(self, rest_accessor):
        body = {
            "error": {
                "code": 403,
                "message": "Forbidden",
                "status": "PERMISSION_DENIED",
            }
        }
        rest_accessor._authed_session._calls.append(DummyResponse(403, body))
        with pytest.raises(GcsClientError) as e:
            rest_accessor.acquire_lock(
                target.AcquireLockRequest(
                    bucket="b",
                    object_key="k",
                    expires_sec=30,
                    owner="alice",
                    force=False,
                )
            )
        assert e.value.status_code == 403

    def test_acquire_lock_5xx(self, rest_accessor):
        rest_accessor._authed_session._calls.append(
            DummyResponse(502, None, text="Bad gateway")
        )
        with pytest.raises(UnexpectedGCSResponseError) as e:
            rest_accessor.acquire_lock(
                target.AcquireLockRequest(
                    bucket="b",
                    object_key="k",
                    expires_sec=30,
                    owner="alice",
                    force=False,
                )
            )
        assert e.value.status_code == 502

    def test_acquire_lock_force_true(self, rest_accessor):
        body = sample_lock_body()
        rest_accessor._authed_session._calls.append(DummyResponse(200, body))
        got = rest_accessor.acquire_lock(
            target.AcquireLockRequest(
                bucket="b", object_key="k", expires_sec=30, owner="alice", force=True
            )
        )
        assert got.lock_owner == "alice"


@pytest.mark.unittest
class TestUpdateLock:
    def test_update_lock_success(self, rest_accessor):
        body = sample_lock_body(owner="alice", generation=2, metageneration=2)
        rest_accessor._authed_session._calls.append(DummyResponse(200, body))
        got = rest_accessor.update_lock(
            target.UpdateLockRequest(
                bucket="b",
                object_key="k",
                metageneration=1,
                expires_sec=30,
                owner="alice",
                force=False,
            )
        )
        assert got.generation == 2
        assert got.metageneration == 2

    def test_update_lock_conflict(self, rest_accessor):
        rest_accessor._authed_session._calls.append(
            DummyResponse(412, {"error": {"code": 412}})
        )
        with pytest.raises(LockConflictError):
            rest_accessor.update_lock(
                target.UpdateLockRequest(
                    bucket="b",
                    object_key="k",
                    metageneration=1,
                    expires_sec=30,
                    owner="alice",
                    force=False,
                )
            )

    def test_update_lock_4xx(self, rest_accessor):
        body = {"error": {"code": 400, "message": "Bad Request"}}
        rest_accessor._authed_session._calls.append(DummyResponse(400, body))
        with pytest.raises(GcsClientError) as e:
            rest_accessor.update_lock(
                target.UpdateLockRequest(
                    bucket="b",
                    object_key="k",
                    metageneration=1,
                    expires_sec=30,
                    owner="alice",
                    force=False,
                )
            )
        assert e.value.status_code == 400

    def test_update_lock_5xx(self, rest_accessor):
        rest_accessor._authed_session._calls.append(
            DummyResponse(503, None, text="Service unavailable")
        )
        with pytest.raises(UnexpectedGCSResponseError) as e:
            rest_accessor.update_lock(
                target.UpdateLockRequest(
                    bucket="b",
                    object_key="k",
                    metageneration=1,
                    expires_sec=30,
                    owner="alice",
                    force=False,
                )
            )
        assert e.value.status_code == 503

    def test_update_lock_force_true(self, rest_accessor):
        body = sample_lock_body(owner="alice", generation=3, metageneration=3)
        rest_accessor._authed_session._calls.append(DummyResponse(200, body))
        got = rest_accessor.update_lock(
            target.UpdateLockRequest(
                bucket="b",
                object_key="k",
                metageneration=1,
                expires_sec=30,
                owner="alice",
                force=True,
            )
        )
        assert got.generation == 3
        assert got.metageneration == 3


@pytest.mark.unittest
class TestReleaseLock:
    def test_release_lock_success(self, rest_accessor):
        rest_accessor._authed_session._calls.append(DummyResponse(204, {}))
        req = target.ReleaseLockRequest(
            bucket="b", object_key="k", generation=1, metageneration=1
        )
        rest_accessor.release_lock(req)  # 例外にならないこと

    def test_release_lock_warn_404(self, rest_accessor):
        rest_accessor._authed_session._calls.append(
            DummyResponse(404, {"error": {"code": 404}})
        )
        req = target.ReleaseLockRequest(
            bucket="b", object_key="k", generation=1, metageneration=1
        )
        rest_accessor.release_lock(req)  # 例外にならないこと

    def test_release_lock_warn_412(self, rest_accessor):
        rest_accessor._authed_session._calls.append(
            DummyResponse(412, {"error": {"code": 412}})
        )
        req = target.ReleaseLockRequest(
            bucket="b", object_key="k", generation=1, metageneration=1
        )
        rest_accessor.release_lock(req)  # 例外にならないこと

    def test_release_lock_4xx(self, rest_accessor):
        body = {"error": {"code": 400, "message": "Bad Request"}}
        rest_accessor._authed_session._calls.append(DummyResponse(400, body))
        req = target.ReleaseLockRequest(
            bucket="b", object_key="k", generation=1, metageneration=1
        )
        with pytest.raises(GcsClientError) as e:
            rest_accessor.release_lock(req)
        assert e.value.status_code == 400

    def test_release_lock_5xx(self, rest_accessor):
        rest_accessor._authed_session._calls.append(
            DummyResponse(500, None, text="Internal error")
        )
        req = target.ReleaseLockRequest(
            bucket="b", object_key="k", generation=1, metageneration=1
        )
        with pytest.raises(UnexpectedGCSResponseError) as e:
            rest_accessor.release_lock(req)
        assert e.value.status_code == 500
