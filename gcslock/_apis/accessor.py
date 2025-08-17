import abc
import json
import os
from json import JSONDecodeError

from google.auth import default
from google.auth.credentials import AnonymousCredentials, Credentials
from google.auth.transport.requests import AuthorizedSession

from .._logger import get_logger
from ..exception import *
from .model import *


class Accessor(metaclass=abc.ABCMeta):
    """
    Abstraction for interacting with the underlying storage system that holds locks.

    Concrete implementations are responsible for talking to the storage backend
    (e.g., Google Cloud Storage) and providing CRUD-like operations for lock objects.
    """

    @abc.abstractmethod
    def bucket_exists(self, request: BucketExistsRequest) -> bool:
        """
        Check whether the target bucket exists.

        Args:
            request: Request containing the bucket to check.

        Returns:
            bool: True if the bucket exists; False otherwise.
        """
        ...

    @abc.abstractmethod
    def get_lock_info(self, request: GetLockInfoRequest) -> LockResponse | None:
        """
        Retrieve the current lock information for an object key.

        Args:
            request: Request specifying bucket and object key for the lock.

        Returns:
            LockResponse | None: The lock info if found; otherwise None.
        """
        ...

    @abc.abstractmethod
    def acquire_lock(self, request: AcquireLockRequest) -> LockResponse:
        """
        Create a new lock if none exists, or force-create when allowed.

        Args:
            request: Request describing the lock to acquire (bucket, key, owner, TTL).

        Returns:
            LockResponse: The resulting lock state as stored by the backend.

        Raises:
            LockConflictError: If the lock already exists and cannot be created.
            GCSApiError: On backend API failures.
        """
        ...

    @abc.abstractmethod
    def update_lock(self, request: UpdateLockRequest) -> LockResponse:
        """
        Refresh/update an existing lock, typically extending its expiration.

        Args:
            request: Request with target lock identity and expected metageneration.

        Returns:
            LockResponse: The updated lock state.

        Raises:
            LockConflictError: If the lock has changed and cannot be updated safely.
            GCSApiError: On backend API failures.
        """
        ...

    @abc.abstractmethod
    def release_lock(self, lock_info: ReleaseLockRequest):
        """
        Release an existing lock using strong conditions (generation/metageneration).

        Args:
            lock_info: Request containing bucket, key, generation, and metageneration.

        Raises:
            LockConflictError: If the lock has changed and cannot be released safely.
            GCSApiError: On backend API failures.
        """
        ...


def _response_to_lock_info(response):
    """
    Translate a backend HTTP response into a LockResponse domain object.

    Expects a JSON body compatible with GCS object resource schema and
    extracts the lock metadata and timestamps.

    Args:
        response: The HTTP response object exposing .json() and fields used below.

    Returns:
        LockResponse: The parsed lock representation.
    """
    resp_body = response.json()
    metadata = resp_body.get("metadata", {})
    expires_sec = int(metadata.get("expires_sec", "0"))
    if expires_sec <= 0:
        expires_sec = 0

    updated_raw = resp_body.get("updated").replace("Z", "+00:00")
    if "." in updated_raw:
        ts_part, rest = updated_raw.split(".")
        frac, tz = rest.split("+")
        frac = frac.ljust(6, "0")
        updated_raw = f"{ts_part}.{frac}+{tz}"

    updated = datetime.fromisoformat(updated_raw)

    return LockResponse(
        bucket=resp_body.get("bucket"),
        object_key=resp_body.get("name"),
        generation=resp_body.get("generation"),
        metageneration=resp_body.get("metageneration"),
        lock_owner=metadata.get("lock_owner"),
        locked_at=updated,
        expires_sec=expires_sec,
    )


def _response_fields() -> set[str]:
    """
    The set of fields to request from the backend to minimize payload size.

    Returns:
        set[str]: Field names required to reconstruct a LockResponse.
    """
    return {"metadata", "generation", "updated", "metageneration", "bucket", "name"}


def _is_client_error(response) -> bool:
    return 400 <= response.status_code < 500


def _extract_error_info(response) -> dict:
    try:
        return response.json().get("error", {})
    except JSONDecodeError:
        return {}


def _handle_error(response) -> GCSApiError:
    error_info = _extract_error_info(response)
    if _is_client_error(response) and "message" in error_info:
        return GcsClientError(
            status_code=response.status_code,
            message=error_info["message"],
            details=error_info,
        )
    return UnexpectedGCSResponseError(
        status_code=response.status_code, response=response.text
    )


class RestAccessor(Accessor):
    """
    Accessor implementation using Google Cloud Storage JSON/JSON+Upload APIs.

    Uses an AuthorizedSession to call GCS endpoints and maps responses to
    the library's domain objects and errors.
    """

    _standard_query_parameters = {
        "projection": "noAcl",
        "fields": ",".join(_response_fields()),
        "prettyPrint": "false",
    }

    def __init__(self, credentials: Credentials | None, logger=None):
        """
        Create a RestAccessor bound to given Google auth credentials.

        Args:
            credentials: Google auth credentials used to authorize requests.
            logger: Optional logger; defaults to the library logger.
        """

        if logger is None:
            logger = get_logger()

        self._logger = logger

        emulator_host = os.getenv("STORAGE_EMULATOR_HOST", None)

        if emulator_host is not None:
            self._base_endpoint = emulator_host
            self._logger.warning("Using GCS emulator host: %s", self._base_endpoint)
            credentials = AnonymousCredentials()
        else:
            self._base_endpoint = "https://storage.googleapis.com"
            if credentials is None:
                credentials, _ = default()

        self._authed_session = AuthorizedSession(credentials=credentials)

    def bucket_exists(self, request: BucketExistsRequest) -> bool:
        """
        Check bucket existence via GCS buckets.get.

        Args:
            request: Bucket existence request.

        Returns:
            bool: True if the bucket exists; False if 404 is returned.

        Raises:
            UnexpectedGCSResponseError: For unexpected status codes.
        """
        endpoint = f"{self._base_endpoint}/storage/v1/b/{request.bucket}"
        response = self._authed_session.get(endpoint, params={"fields": "name"})

        self._logger.debug(
            f"GCS bucket exists endpoint: {endpoint}, params {response.request.body}, response {response.text}"
        )

        if response.status_code == 404:
            return False
        elif response.status_code == 200:
            return True
        else:
            raise _handle_error(response)

    def get_lock_info(self, request: GetLockInfoRequest) -> LockResponse | None:
        endpoint = f"{self._base_endpoint}/storage/v1/b/{request.bucket}/o/{request.object_key}"
        query_params = {**self._standard_query_parameters}

        response = self._authed_session.get(
            endpoint,
            params=query_params,
        )

        self._logger.debug(
            f"GCS get lock info endpoint: {endpoint}, params {response.request.body}, response {response.text}"
        )

        if response.status_code == 404:
            return None
        elif response.status_code == 200:
            return _response_to_lock_info(response)
        else:
            raise _handle_error(response)

    def acquire_lock(self, request: AcquireLockRequest) -> LockResponse:
        endpoint = f"{self._base_endpoint}/upload/storage/v1/b/{request.bucket}/o"
        object_resource = {
            "name": request.object_key,
            "contentType": "text/plain",
            "metadata": request.to_metadata(),
        }
        query_params = {
            **self._standard_query_parameters,
            "name": request.object_key,
            "uploadType": "multipart",
            "ifGenerationMatch": 0,
        }

        if request.force:
            del query_params["ifGenerationMatch"]

        boundary_string = "separate_string"
        multipart_data = (
            f"--{boundary_string}\r\n"
            "Content-Type: application/json\r\n\r\n"
            f"{json.dumps(object_resource, ensure_ascii=False)}\r\n"
            f"--{boundary_string}\r\n"
            "Content-Type: text/plain\r\n\r\n"
            "lock\r\n"
            f"--{boundary_string}--\r\n"
        ).encode("utf-8")

        headers = {
            "Content-Type": f"multipart/related; boundary={boundary_string}",
            "Content-Length": str(len(multipart_data)),
        }

        response = self._authed_session.post(
            endpoint, data=multipart_data, params=query_params, headers=headers
        )

        self._logger.debug(
            f"GCS acquire lock endpoint: {endpoint}, params {response.request.body}, response {response.text}"
        )

        if response.status_code == 200:
            return _response_to_lock_info(response)
        elif response.status_code == 412:
            raise LockConflictError(
                bucket_name=request.bucket, lock_id=request.object_key
            )
        else:
            raise _handle_error(response)

    def update_lock(self, request: UpdateLockRequest) -> LockResponse:
        endpoint = f"{self._base_endpoint}/storage/v1/b/{request.bucket}/o/{request.object_key}"
        query_params = {
            **self._standard_query_parameters,
            "ifMetagenerationMatch": request.metageneration,
        }

        request_data = {
            "metadata": request.to_metadata(),
        }

        if request.force:
            del query_params["ifMetagenerationMatch"]

        response = self._authed_session.patch(
            endpoint, params=query_params, json=request_data
        )

        self._logger.debug(
            f"GCS update lock endpoint: {endpoint}, params {response.request.body}, response {response.text}"
        )

        if response.status_code == 200:
            return _response_to_lock_info(response)
        elif response.status_code == 412:
            raise LockConflictError(
                bucket_name=request.bucket, lock_id=request.object_key
            )
        else:
            raise _handle_error(response)

    def release_lock(self, request: ReleaseLockRequest):
        endpoint = f"{self._base_endpoint}/storage/v1/b/{request.bucket}/o/{request.object_key}"
        query_params = {
            **self._standard_query_parameters,
            "ifMetagenerationMatch": request.metageneration,
            "ifGenerationMatch": request.generation,
        }

        response = self._authed_session.delete(endpoint, params=query_params)

        self._logger.debug(
            f"GCS release lock endpoint: {endpoint}, params {response.request.body}, response {response.text}"
        )

        if response.status_code in (200, 204):
            return
        elif response.status_code in (404, 412):
            self._logger.warning(
                f"This lock has already been released by another user."
            )
        else:
            raise _handle_error(response)
