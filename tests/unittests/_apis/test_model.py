from dataclasses import FrozenInstanceError
from unittest.mock import patch

import pytest

from gcslock._apis.model import *


@pytest.mark.unittest
class TestAcquireLockRequest:
    def test_init_required_attribute(self):
        acquire = AcquireLockRequest(bucket="example", object_key="test", owner="owner")
        assert acquire.bucket == "example"
        assert acquire.object_key == "test"
        assert acquire.owner == "owner"
        assert acquire.expires_sec == 20
        assert acquire.force is False

    def test_init_optional_attribute(self):
        acquire = AcquireLockRequest(
            bucket="example",
            object_key="test",
            owner="owner",
            expires_sec=10,
            force=True,
        )
        assert acquire.bucket == "example"
        assert acquire.object_key == "test"
        assert acquire.owner == "owner"
        assert acquire.expires_sec == 10
        assert acquire.force is True

    def test_to_metadata(self):
        acquire = AcquireLockRequest(
            bucket="example",
            object_key="test",
            owner="owner",
            expires_sec=10,
            force=True,
        )
        metadata = acquire.to_metadata()
        assert metadata == {
            "expires_sec": "10",
            "lock_owner": "owner",
        }

    def test_to_metadata_owner_none(self):
        fake_uuid = "1234567890abcdef"
        with patch("gcslock._apis.model.uuid.uuid4") as mock_uuid4:

            class _U:
                hex = fake_uuid

            mock_uuid4.return_value = _U()
            acquire = AcquireLockRequest(bucket="b", object_key="o", owner=None)
            metadata = acquire.to_metadata()
            assert metadata == {"expires_sec": "20", "lock_owner": fake_uuid}

    def test_frozen_dataclass_immutable(self):
        acquire = AcquireLockRequest(bucket="b", object_key="o", owner="alice")
        with pytest.raises(FrozenInstanceError):
            acquire.owner = "bob"  # type: ignore[attr-defined]


@pytest.mark.unittest
class TestUpdateLockRequest:
    def test_init_required_attribute(self):
        acquire = UpdateLockRequest(
            bucket="example", object_key="test", metageneration=1, owner="owner"
        )
        assert acquire.bucket == "example"
        assert acquire.object_key == "test"
        assert acquire.metageneration == 1
        assert acquire.owner == "owner"
        assert acquire.expires_sec == 20
        assert acquire.force is False

    def test_init_optional_attribute(self):
        acquire = UpdateLockRequest(
            bucket="example",
            object_key="test",
            metageneration=2,
            owner="owner",
            expires_sec=10,
            force=True,
        )
        assert acquire.bucket == "example"
        assert acquire.object_key == "test"
        assert acquire.metageneration == 2
        assert acquire.owner == "owner"
        assert acquire.expires_sec == 10
        assert acquire.force is True

    def test_to_metadata(self):
        acquire = UpdateLockRequest(
            bucket="example",
            object_key="test",
            metageneration=1,
            owner="owner",
            expires_sec=10,
            force=True,
        )
        metadata = acquire.to_metadata()
        assert metadata == {
            "expires_sec": "10",
            "lock_owner": "owner",
        }

    def test_frozen_dataclass_immutable(self):
        req = UpdateLockRequest(
            bucket="b", object_key="o", metageneration=42, owner="a"
        )
        with pytest.raises(FrozenInstanceError):
            req.metageneration = 99  # type: ignore[attr-defined]


@pytest.mark.unittest
class TestLockResponse:
    @pytest.fixture(scope="class")
    def test_datetime(self):
        return datetime.fromisoformat("2025-01-01T00:00:00+00:00")

    def test_init_required_attribute(self, test_datetime):
        lock_response = LockResponse(
            bucket="example",
            object_key="test",
            generation=1,
            metageneration=1,
            lock_owner="owner",
            locked_at=test_datetime,
            expires_sec=10,
        )
        assert lock_response.bucket == "example"
        assert lock_response.object_key == "test"
        assert lock_response.generation == 1
        assert lock_response.metageneration == 1
        assert lock_response.lock_owner == "owner"
        assert lock_response.locked_at == test_datetime
        assert lock_response.expires_sec == 10

    @pytest.mark.parametrize(
        ("expires_sec",),
        [
            (10,),
            (20,),
            (60,),
        ],
    )
    def test_expires_at(self, test_datetime, expires_sec: int):
        lock_response = LockResponse(
            bucket="example",
            object_key="test",
            generation=1,
            metageneration=1,
            lock_owner="owner",
            locked_at=test_datetime,
            expires_sec=expires_sec,
        )
        assert lock_response.expires_at == test_datetime + timedelta(
            seconds=expires_sec
        )

    @pytest.mark.parametrize(
        ("current", "owner", "is_expired"),
        [
            (datetime.fromisoformat("2025-01-01T00:00:09+00:00"), "owner", True),
            (datetime.fromisoformat("2025-01-01T00:00:09+00:00"), "user", False),
            (datetime.fromisoformat("2025-01-01T00:00:10+00:00"), "user", False),
            (datetime.fromisoformat("2025-01-01T00:00:11+00:00"), "user", True),
        ],
    )
    def test_can_be_acquire_by(
        self,
        monkeypatch,
        test_datetime: datetime,
        current: datetime,
        owner: str,
        is_expired: bool,
    ):
        expires_sec = 10
        with patch("gcslock._apis.model.datetime") as mock_datetime:
            mock_datetime.now.return_value = current
            lock_response = LockResponse(
                bucket="example",
                object_key="test",
                generation=1,
                metageneration=1,
                lock_owner="owner",
                locked_at=test_datetime,
                expires_sec=expires_sec,
            )
            assert lock_response.can_be_acquire_by(owner) is is_expired

    def test_to_metadata(self):
        lock_response = LockResponse(
            bucket="example",
            object_key="test",
            generation=1,
            metageneration=1,
            lock_owner="owner",
            locked_at=datetime.now(timezone.utc),
            expires_sec=10,
        )
        assert lock_response.to_metadata() == {
            "expires_sec": "10",
            "lock_owner": "owner",
        }

    def test_is_expired_direct_true_false(self, test_datetime):
        lr = LockResponse(
            bucket="b",
            object_key="o",
            generation=1,
            metageneration=1,
            lock_owner="alice",
            locked_at=test_datetime,
            expires_sec=10,
        )
        with patch("gcslock._apis.model.datetime") as mock_datetime:
            mock_datetime.now.return_value = test_datetime + timedelta(seconds=5)
            assert lr.is_expired() is False

        with patch("gcslock._apis.model.datetime") as mock_datetime:
            mock_datetime.now.return_value = test_datetime + timedelta(seconds=15)
            assert lr.is_expired() is True

    def test_can_be_acquire_by_owner_always_true(self, test_datetime):
        lr = LockResponse(
            bucket="b",
            object_key="o",
            generation=1,
            metageneration=1,
            lock_owner="alice",
            locked_at=test_datetime,
            expires_sec=1,
        )
        # オーナー一致の場合は期限に関係なく True
        assert lr.can_be_acquire_by("alice") is True

    def test_frozen_dataclass_immutable(self, test_datetime):
        lr = LockResponse(
            bucket="b",
            object_key="o",
            generation=1,
            metageneration=1,
            lock_owner="alice",
            locked_at=test_datetime,
            expires_sec=10,
        )
        with pytest.raises(FrozenInstanceError):
            lr.lock_owner = "bob"  # type: ignore[attr-defined]


@pytest.mark.unittest
class TestBucketExistsRequest:
    def test_init_and_attributes(self):
        req = BucketExistsRequest(bucket="my-bucket")
        assert req.bucket == "my-bucket"


@pytest.mark.unittest
class TestGetLockInfoRequest:
    def test_init_and_attributes(self):
        req = GetLockInfoRequest(bucket="b", object_key="k")
        assert req.bucket == "b"
        assert req.object_key == "k"


@pytest.mark.unittest
class TestReleaseLockRequest:
    def test_init_and_attributes(self):
        req = ReleaseLockRequest(
            bucket="b", object_key="o", generation=1, metageneration=2
        )
        assert req.bucket == "b"
        assert req.object_key == "o"
        assert req.generation == 1
        assert req.metageneration == 2

    def test_frozen_dataclass_immutable(self):
        req = ReleaseLockRequest(
            bucket="b", object_key="o", generation=1, metageneration=2
        )
        with pytest.raises(FrozenInstanceError):
            req.generation = 99  # type: ignore[attr-defined]
