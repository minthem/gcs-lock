import abc
import json

from google.auth.credentials import Credentials
from google.auth.transport.requests import AuthorizedSession

from .._logger import get_logger
from ..exception import *
from .model import *


class Accessor(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def bucket_exists(self, request: BucketExistsRequest) -> bool: ...

    @abc.abstractmethod
    def get_lock_info(self, request: GetLockInfoRequest) -> LockResponse | None: ...

    @abc.abstractmethod
    def acquire_lock(self, request: AcquireLockRequest) -> LockResponse: ...

    @abc.abstractmethod
    def update_lock(self, request: UpdateLockRequest) -> LockResponse: ...

    @abc.abstractmethod
    def release_lock(self, lock_info: ReleaseLockRequest): ...


def _response_to_lock_info(response):
    resp_body = response.json()
    metadata = resp_body.get("metadata", {})
    expires_sec = int(metadata.get("expires_sec", "0"))
    if expires_sec <= 0:
        expires_sec = 0
    updated = datetime.fromisoformat(resp_body.get("updated").replace("Z", "+00:00"))

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
    return {"metadata", "generation", "updated", "metageneration", "bucket", "name"}


class AuthedRestAccessor(Accessor):

    _base_endpoint = "https://storage.googleapis.com"
    _standard_query_parameters = {
        "projection": "noAcl",
        "fields": ",".join(_response_fields()),
        "prettyPrint": "false",
    }

    def __init__(self, credentials: Credentials, logger=None):
        self._authed_session = AuthorizedSession(credentials=credentials)

        if logger is None:
            logger = get_logger()

        self._logger = logger

    def bucket_exists(self, request: BucketExistsRequest) -> bool:
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
            response.raise_for_status()
            raise RuntimeError("Unexpected response from GCS")

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
            response.raise_for_status()
            raise RuntimeError("Unexpected response from GCS")

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

        boundary_string = "separator_string"
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

        if response.status_code == 200:
            return _response_to_lock_info(response)
        elif response.status_code == 412:
            raise LockConflictError(
                bucket_name=request.bucket, lock_id=request.object_key
            )
        else:
            raise UnexpectedGCSResponseError(
                status_code=response.status_code, response=response.text
            )

    def update_lock(self, request: UpdateLockRequest) -> LockResponse:
        endpoint = f"{self._base_endpoint}/storage/v1/b/{request.bucket}/o/{request.object_key}"
        query_params = {
            **self._standard_query_parameters,
            "ifMetagenerationMatch": request.metageneration,
            "metadata": request.to_metadata(),
        }

        if request.force:
            del query_params["ifMetagenerationMatch"]

        response = self._authed_session.patch(endpoint, params=query_params)

        if response.status_code == 200:
            return _response_to_lock_info(response)
        elif response.status_code == 412:
            raise LockConflictError(
                bucket_name=request.bucket, lock_id=request.object_key
            )
        else:
            raise UnexpectedGCSResponseError(
                status_code=response.status_code, response=response.text
            )

    def release_lock(self, request: ReleaseLockRequest):
        endpoint = f"{self._base_endpoint}/storage/v1/b/{request.bucket}/o/{request.object_key}"
        query_params = {
            **self._standard_query_parameters,
            "ifMetagenerationMatch": request.metageneration,
            "ifGenerationMatch": request.generation,
        }

        response = self._authed_session.delete(endpoint, params=query_params)

        if response.status_code == 204:
            return
        elif response.status_code in (404, 412):
            self._logger.warn(f"This lock has already been released by another user.")
        else:
            raise UnexpectedGCSResponseError(
                status_code=response.status_code, response=response.text
            )
