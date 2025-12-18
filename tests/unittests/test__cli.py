from subprocess import CalledProcessError
from unittest.mock import MagicMock

import pytest

from gcslock import _cli


@pytest.fixture(autouse=True)
def mock_gcs(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(_cli, "GcsLock", mock)
    yield mock


@pytest.fixture(autouse=True)
def mock_process(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(_cli, "subprocess", mock)
    yield mock


class TestParseArgs:
    def test_parse_args_all(self):
        args = _cli.parse_args(
            [
                "--bucket",
                "bkt",
                "--object-key",
                "obj",
                "--expires-sec",
                "30",
                "--owner",
                "owner",
                "--wait-sec",
                "5",
                "--service-account-json",
                "/tmp/service_account.json",
                "echo",
                "hello",
            ]
        )
        assert args.bucket == "bkt"
        assert args.object_key == "obj"
        assert args.expires_sec == 30
        assert args.owner == "owner"
        assert args.wait_sec == 5
        assert args.service_account_json == "/tmp/service_account.json"
        assert args.command == ["echo", "hello"]

    def test_parse_args_short(self):
        args = _cli.parse_args(
            [
                "-b",
                "bkt",
                "-k",
                "obj",
                "-e",
                "30",
                "-o",
                "owner",
                "-w",
                "5",
                "--service-account-json",
                "/tmp/service_account.json",
                "echo",
                "hello",
            ]
        )
        assert args.bucket == "bkt"
        assert args.object_key == "obj"
        assert args.expires_sec == 30
        assert args.owner == "owner"
        assert args.wait_sec == 5
        assert args.service_account_json == "/tmp/service_account.json"
        assert args.command == ["echo", "hello"]

    def test_parse_args_minimal(self):
        args = _cli.parse_args(
            [
                "--bucket",
                "bkt",
                "--object-key",
                "obj",
                "echo",
                "hello",
            ]
        )
        assert args.bucket == "bkt"
        assert args.object_key == "obj"
        assert args.expires_sec == 20
        assert args.owner is None
        assert args.wait_sec == 0
        assert args.service_account_json is None
        assert args.command == ["echo", "hello"]

    def test_parse_args_bucket_not_specific(self):
        with pytest.raises(SystemExit):
            _cli.parse_args(
                [
                    "--object-key",
                    "obj",
                    "echo",
                    "hello",
                ]
            )

    def test_parse_args_object_not_specific(self):
        with pytest.raises(SystemExit):
            _cli.parse_args(
                [
                    "--bucket",
                    "bkt",
                    "echo",
                    "hello",
                ]
            )

    def test_parse_args_command_not_specific(self):
        with pytest.raises(SystemExit):
            _cli.parse_args(
                [
                    "--bucket",
                    "bkt",
                    "--object-key",
                    "obj",
                ]
            )

    def test_parse_args_expires_sec_invalid_type(self):
        with pytest.raises(SystemExit):
            _cli.parse_args(
                [
                    "--bucket",
                    "bkt",
                    "--object-key",
                    "obj",
                    "--expires-sec",
                    "a",
                    "echo",
                    "hello",
                ]
            )

    def test_parse_args_wait_sec_invalid_type(self):
        with pytest.raises(SystemExit):
            _cli.parse_args(
                [
                    "--bucket",
                    "bkt",
                    "--object-key",
                    "obj",
                    "--wait-sec",
                    "a",
                    "echo",
                    "hello",
                ]
            )


class TestMain:
    def test_main_success_with_service_account(
        self, mock_gcs, mock_process, monkeypatch
    ):
        creds = MagicMock(name="Credentials")
        monkeypatch.setattr(
            _cli.service_account.Credentials,
            "from_service_account_file",
            MagicMock(return_value=creds),
        )

        lock_instance = mock_gcs.return_value
        ctx = MagicMock()
        ctx.__enter__.return_value = None
        lock_instance.acquire.return_value = ctx

        argv = [
            "--bucket",
            "bkt",
            "--object-key",
            "obj",
            "--expires-sec",
            "30",
            "--owner",
            "owner",
            "--wait-sec",
            "5",
            "--service-account-json",
            "/tmp/service_account.json",
            "echo",
            "hello",
        ]

        _cli.main(argv)

        _cli.service_account.Credentials.from_service_account_file.assert_called_once_with(
            "/tmp/service_account.json"
        )
        mock_gcs.assert_called_once_with(lock_owner="owner", credentials=creds)
        lock_instance.acquire.assert_called_once_with(
            "bkt", "obj", 30, max_wait_seconds=5
        )
        mock_process.run.assert_called_once_with(["echo", "hello"], check=True)

    def test_main_error_exit_when_acquire_fails(self, mock_gcs, capsys):
        lock_instance = mock_gcs.return_value
        ctx = MagicMock()
        ctx.__enter__.side_effect = Exception("failed to acquire")
        lock_instance.acquire.return_value = ctx

        with pytest.raises(SystemExit) as e:
            _cli.main(
                [
                    "--bucket",
                    "bkt",
                    "--object-key",
                    "obj",
                    "echo",
                    "world",
                ]
            )

        assert e.value.code == 1
        err = capsys.readouterr().err
        assert "failed to acquire" in err
        mock_gcs.assert_called_once_with(lock_owner=None, credentials=None)
        lock_instance.acquire.assert_called_once_with(
            "bkt", "obj", 20, max_wait_seconds=0
        )

    def test_main_error_exit_when_command_fails(self, mock_gcs, mock_process, capsys):
        lock_instance = mock_gcs.return_value
        ctx = MagicMock()
        ctx.__enter__.return_value = None
        lock_instance.acquire.return_value = ctx
        mock_process.run.side_effect = CalledProcessError(
            returncode=1,
            cmd="fail command",
            output=b"Command 'exit 1' returned non-zero exit status 1",
        )

        with pytest.raises(SystemExit) as e:
            _cli.main(["--bucket", "bkt", "--object-key", "obj", "fail", "command"])

        assert e.value.code == 1
        err = capsys.readouterr().err
        assert "Command 'fail command'" in err
