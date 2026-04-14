from __future__ import annotations

from pathlib import Path

import pytest

from sketch2_test_utils import extension_path, import_duckdb_client


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def sketch2_extension_path() -> Path:
    path = extension_path()
    if not path.exists():
        pytest.fail(f"Sketch2 extension binary not found at {path}. Run `CCACHE_DISABLE=1 make` first.")
    return path


@pytest.fixture(scope="session")
def duckdb_client():
    try:
        return import_duckdb_client()
    except ImportError as exc:
        pytest.fail(str(exc))


@pytest.fixture()
def duckdb_con(duckdb_client, sketch2_extension_path):
    con = duckdb_client.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    try:
        con.execute(f"LOAD '{sketch2_extension_path.as_posix()}'")
        yield con
    finally:
        con.close()
