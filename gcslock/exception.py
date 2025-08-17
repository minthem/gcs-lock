from typing import Any


class GcsLockError(Exception):
    """Base class for errors raised by the GCS Lock library."""


class GCSApiError(GcsLockError):
    """Base class for errors raised when interacting with GCS APIs."""

    def __init__(self, status_code: int, message: str = ""):
        super().__init__(f"GCS API error ({status_code}): {message}")
        self.status_code = status_code
        self.message = message


class BucketNotFoundError(GCSApiError):
    """Raised when a specified bucket does not exist."""

    def __init__(self, bucket_name: str):
        super().__init__(404, f"Bucket '{bucket_name}' not found.")
        self.bucket_name = bucket_name


class LockConflictError(GCSApiError):
    """Raised when trying to update or release a lock that has been modified or deleted."""

    def __init__(self, bucket_name: str, lock_id: str):
        super().__init__(
            412,
            f"Lock on object '{lock_id}' in bucket '{bucket_name}' is no longer valid.",
        )
        self.bucket_name = bucket_name
        self.lock_id = lock_id


class GcsClientError(GCSApiError):
    """Raised when an error occurs while interacting with the GCS client."""

    def __init__(self, status_code: int, message: str, details: dict[str, Any]):
        super().__init__(status_code, message)
        self.details = details


class UnexpectedGCSResponseError(GCSApiError):
    """Raised for non-explicit GCS failures not mapped to a known domain error."""

    def __init__(self, status_code: int, response: str | Any):
        super().__init__(status_code, f"Unexpected response from GCS: {response}")
        self.response = response


class LockNotHeldError(GcsLockError):
    def __init__(self, lock_state_id: str):
        super().__init__(
            f"Lock '{lock_state_id}' was not acquired or already released by this instance"
        )
        self.lock_state_id = lock_state_id
