import os
import subprocess
import time
from typing import Generator

import pytest
import requests
from google.auth.credentials import Credentials
from pytest import MonkeyPatch

FAKE_GCS_IMAGE = "fsouza/fake-gcs-server:latest"
FAKE_GCS_PORT = 4443  # デフォルトの JSON API ポート（HTTP）
FAKE_GCS_HOST = f"http://localhost:{FAKE_GCS_PORT}"
STORAGE_EMULATOR_HOST_ENV = "STORAGE_EMULATOR_HOST"


class DummyCredentials(Credentials):
    """
    ネットワークを使わないダミー認証情報。
    - token のみを持ち、expiry は None（常に非期限切れ扱い）
    - refresh は Request を使わずローカル更新のみ
    - apply で Authorization ヘッダを付与
    """

    def __init__(self):
        super().__init__()
        self.token = "initial-token"
        self.expiry = None  # 期限なし（valid 判定で refresh されない）
        self.refresh_called_with = None

    def refresh(self, request):
        # リフレッシュ要求が来てもネットワークは使わない
        self.refresh_called_with = request
        self.token = "refreshed-token"
        self.expiry = None  # 引き続き期限なし

    def apply(self, headers, token=None):
        headers["authorization"] = f"Bearer {token or self.token}"

    def with_quota_project(self, quota_project_id):
        return self


def _wait_for_emulator_ready(base_url: str, timeout: float = 10.0) -> None:
    """
    ヘルスチェック: バケット一覧 API に GET を投げて 200 が返るまで待機
    """
    deadline = time.time() + timeout
    url = f"{base_url}/storage/v1/b"
    last_err = None
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=1.0)
            if r.status_code == 200:
                return
        except Exception as e:
            last_err = e
        time.sleep(0.3)
    raise RuntimeError(f"GCS emulator not ready in time: last_err={last_err}")


@pytest.fixture(scope="session")
def gcs_emulator() -> Generator[dict, None, None]:
    """
    fake-gcs-server を Docker で起動するフィクスチャ。
    戻り値としてエンドポイント（base_url, json_api_base）を返す。
    """
    # 既に動いていれば再利用（CI などで事前起動しているケース）
    try:
        _wait_for_emulator_ready(FAKE_GCS_HOST, timeout=2.0)
        started = False
        proc = None
    except Exception:
        # 起動していなければ起動
        cmd = [
            "docker",
            "run",
            "--rm",
            "-p",
            f"{FAKE_GCS_PORT}:4443",
            FAKE_GCS_IMAGE,
            "-scheme",
            "http",
            "-port",
            "4443",
            "-public-host",
            f"localhost:{FAKE_GCS_PORT}",
            "-backend",
            "memory",
        ]
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        started = True
        try:
            _wait_for_emulator_ready(FAKE_GCS_HOST, timeout=10.0)
        except Exception:
            # ログを出して失敗させる
            if proc and proc.stdout:
                try:
                    print(proc.stdout.read())
                except Exception:
                    pass
            if proc:
                proc.terminate()
            raise

    # エミュレータ用ホストを環境変数に設定
    os.environ[STORAGE_EMULATOR_HOST_ENV] = FAKE_GCS_HOST

    try:
        yield {
            "base_url": FAKE_GCS_HOST,
            "json_api_base": f"{FAKE_GCS_HOST}/storage/v1",
        }
    finally:
        if started and proc:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


@pytest.fixture(autouse=True, scope="session")
def gcp_auth_patch():
    """
    すべてのテストで google.auth.default をダミーに差し替える。
    session スコープのため、pytest.MonkeyPatch を直接使い最後に undo する。
    """
    from gcslock import core as gcs_core

    def fake_default():
        return DummyCredentials(), "test-project"

    mp = MonkeyPatch()
    mp.setattr(gcs_core, "default", fake_default)
    try:
        yield
    finally:
        mp.undo()
