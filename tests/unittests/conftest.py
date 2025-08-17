import os

import pytest


@pytest.fixture(autouse=True, scope="session")
def _set_emulator_env():
    os.environ.setdefault("STORAGE_EMULATOR_HOST", "http://localhost:4443")
    yield
    os.environ.pop("STORAGE_EMULATOR_HOST", None)
