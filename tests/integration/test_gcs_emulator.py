import time
import uuid

import pytest
import requests

import gcslock


def create_bucket(json_api_base: str, bucket_name: str) -> None:
    """
    fake-gcs-server の JSON API に直接 POST してバケットを作る。
    """
    url = f"{json_api_base}/b"
    params = {"project": "test-project"}
    payload = {"name": bucket_name}
    r = requests.post(url, params=params, json=payload, timeout=3.0)
    if r.status_code not in (200, 409):
        # 200: 作成成功, 409: 既に存在
        raise AssertionError(f"Failed to create bucket: {r.status_code} {r.text}")


@pytest.mark.integration
def test_lock_acquire_release_smoke(gcs_emulator):
    """
    GCS エミュレータを使ったロック取得/解放のスモークテスト。
    認証は DummyCredentials（外部通信なし）に差し替え済み（autouse fixture）。
    """
    bucket_name = f"it-{uuid.uuid4().hex[:8]}"
    create_bucket(gcs_emulator["json_api_base"], bucket_name)

    lock = gcslock.core.GcsLock(bucket_name=bucket_name, lock_owner="it")

    key = "sample-lock"
    ttl_seconds = 2

    with lock.acquire(lock_id=key, expires_seconds=ttl_seconds) as lock_state:
        assert lock_state.bucket == bucket_name
        assert lock_state.lock_id == key
        assert lock_state.lock_owner == "it"
        assert lock_state.expires_at is not None
        assert lock_state.lock_state_id == f"{bucket_name}:{key}"


@pytest.mark.integration
def test_lock_ttl_expiration(gcs_emulator):
    """
    TTL が切れた後に別オーナーが取得できることの検証。
    """
    bucket_name = f"it-{uuid.uuid4().hex[:8]}"
    create_bucket(gcs_emulator["json_api_base"], bucket_name)

    lock_a = gcslock.core.GcsLock(bucket_name=bucket_name, lock_owner="owner-a")
    lock_b = gcslock.core.GcsLock(bucket_name=bucket_name, lock_owner="owner-b")

    key = "ttl-lock"
    ttl_seconds = 2

    lock_a.acquire(lock_id=key, expires_seconds=ttl_seconds)

    # TTL が切れるまで待つ
    time.sleep(ttl_seconds + 0.01)

    # 別オーナーで取得可能に
    acquired_by_b = lock_b.acquire(lock_id=key, expires_seconds=ttl_seconds)
    lock_b.release(acquired_by_b)


@pytest.mark.integration
def test_lock_conflict_with_other_owner(gcs_emulator):
    """
    ロック後に別ユーザーがロック取得を行うと、競合で例外が投げられること。
    """
    bucket_name = f"it-{uuid.uuid4().hex[:8]}"
    create_bucket(gcs_emulator["json_api_base"], bucket_name)

    key = "conflict-lock"
    ttl_seconds = 5

    lock_a = gcslock.core.GcsLock(bucket_name=bucket_name, lock_owner="owner-a")
    lock_b = gcslock.core.GcsLock(bucket_name=bucket_name, lock_owner="owner-b")

    # A がロックを取得
    state_a = lock_a.acquire(lock_id=key, expires_seconds=ttl_seconds)

    # B はロック取得に失敗（競合）
    with pytest.raises(gcslock.exception.LockConflictError):
        lock_b.acquire(lock_id=key, expires_seconds=ttl_seconds)

    # 後片付け
    lock_a.release(state_a)


@pytest.mark.integration
def test_reacquire_same_owner_before_expiry(gcs_emulator):
    """
    同一ユーザーの場合は、期限切れ前に再度ロックを取得（更新）できること。
    """
    bucket_name = f"it-{uuid.uuid4().hex[:8]}"
    create_bucket(gcs_emulator["json_api_base"], bucket_name)

    key = "reacquire-lock"
    ttl_seconds = 5

    lock = gcslock.core.GcsLock(bucket_name=bucket_name, lock_owner="same-owner")

    # 1 回目の取得
    state1 = lock.acquire(lock_id=key, expires_seconds=ttl_seconds)
    assert state1.lock_owner == "same-owner"

    # 少し待ってから（期限前）再取得（更新）
    time.sleep(0.1)
    state2 = lock.acquire(lock_id=key, expires_seconds=ttl_seconds)

    # 同一キー・同一オーナーで取得でき、期限が延長されている（同等以上）
    assert state2.lock_id == key
    assert state2.lock_owner == "same-owner"
    assert state2.expires_at >= state1.expires_at

    # 後片付け（最新状態で解放）
    lock.release(state2)


@pytest.mark.integration
def test_lock_conflict_wait(gcs_emulator):
    """
    ロック後に別ユーザーがロック取得を行うと、競合で例外が投げられること。
    """
    bucket_name = f"it-{uuid.uuid4().hex[:8]}"
    create_bucket(gcs_emulator["json_api_base"], bucket_name)

    key = "conflict-lock"
    ttl_seconds = 2

    lock_a = gcslock.core.GcsLock(bucket_name=bucket_name, lock_owner="owner-a")
    lock_b = gcslock.core.GcsLock(bucket_name=bucket_name, lock_owner="owner-b")

    # A がロックを取得
    state_a = lock_a.acquire(lock_id=key, expires_seconds=ttl_seconds)

    # B はロック取得出来るまで待機
    lock_b.acquire(lock_id=key, expires_seconds=ttl_seconds, max_wait_seconds=5)
