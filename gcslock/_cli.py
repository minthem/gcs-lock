import argparse
import subprocess
import sys
from dataclasses import dataclass

from google.oauth2 import service_account

from gcslock import GcsLock


@dataclass(frozen=True)
class CliArgs:
    bucket: str
    object_key: str
    expires_sec: int
    wait_sec: int
    command: list[str]
    owner: str | None = None
    service_account_json: str | None = None


def parse_args(argv: list[str] | None = None) -> CliArgs:
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser()

    parser.add_argument("--bucket", "-b", required=True, help="The GCS bucket name.")
    parser.add_argument("--object-key", "-k", required=True, help="The object key.")
    parser.add_argument(
        "--expires-sec", "-e", type=int, default=20, help="The lock expiration time."
    )
    parser.add_argument("--owner", "-o", required=False, help="The lock owner.")
    parser.add_argument("--wait-sec", "-w", type=int, default=0, help="Lock wait time.")
    parser.add_argument(
        "--service-account-json", required=False, help="The service account json."
    )
    parser.add_argument(
        "command",
        nargs="+",
        help="The command to execute.",
    )

    args = parser.parse_args(argv)
    return CliArgs(**vars(args))


def main(argv: list[str] | None = None):
    try:
        cli_args = parse_args(argv)

        credentials = None
        if cli_args.service_account_json is not None:
            credentials = service_account.Credentials.from_service_account_file(
                cli_args.service_account_json
            )

        lock = GcsLock(lock_owner=cli_args.owner, credentials=credentials)

        try:
            with lock.acquire(
                cli_args.bucket,
                cli_args.object_key,
                cli_args.expires_sec,
                max_wait_seconds=cli_args.wait_sec,
            ):
                subprocess.run(cli_args.command, check=True)

        except Exception as e:
            print(e, file=sys.stderr)
            sys.exit(1)

    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
