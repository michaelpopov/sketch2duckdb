from __future__ import annotations

import csv
import ctypes
import importlib
import importlib.machinery
import importlib.util
import os
import sys
from ctypes import c_char_p, c_int, c_uint, c_uint64, c_void_p
from pathlib import Path
from typing import Iterable


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def extension_path() -> Path:
    return repo_root() / "build" / "release" / "extension" / "sketch2" / "sketch2.duckdb_extension"


def import_duckdb_client():
    module = importlib.import_module("duckdb")
    if hasattr(module, "connect"):
        return module

    search_paths = []
    repo = repo_root().resolve()
    for entry in sys.path:
        resolved = Path(entry or ".").resolve()
        if resolved == repo:
            continue
        search_paths.append(entry)

    spec = importlib.machinery.PathFinder.find_spec("duckdb", search_paths)
    if spec is None or spec.loader is None or getattr(spec, "origin", None) is None:
        raise ImportError(
            "Python duckdb client package is not installed. "
            "Run `make python-test-deps` to install the repo's Python test dependencies."
        )

    module = importlib.util.module_from_spec(spec)
    sys.modules["duckdb"] = module
    spec.loader.exec_module(module)
    if not hasattr(module, "connect"):
        raise ImportError("Resolved a duckdb module, but it does not expose duckdb.connect")
    return module


def libsketch2_path() -> Path:
    candidates = []

    env_root = Path(os.environ["SKETCH2_ROOT"]) if "SKETCH2_ROOT" in os.environ else None
    if env_root is not None:
        candidates.append(env_root / "install" / "lib" / "libsketch2.so")

    candidates.append(repo_root().parent / "sketch2" / "install" / "lib" / "libsketch2.so")

    for candidate in candidates:
        if candidate.exists():
            return candidate

    searched = ", ".join(str(candidate) for candidate in candidates)
    raise FileNotFoundError(f"libsketch2.so not found. Checked: {searched}")


class Sketch2ApiError(RuntimeError):
    pass


class Sketch2Api:
    def __init__(self, database_root: Path):
        self.database_root = Path(database_root)
        self.lib = ctypes.CDLL(str(libsketch2_path()))
        self._configure()
        self.handle = self.lib.sk_new_handle(str(self.database_root).encode("utf-8"))
        if not self.handle:
            raise Sketch2ApiError("sk_new_handle returned NULL")

    def _configure(self) -> None:
        self.lib.sk_new_handle.argtypes = [c_char_p]
        self.lib.sk_new_handle.restype = c_void_p

        self.lib.sk_release_handle.argtypes = [c_void_p]
        self.lib.sk_release_handle.restype = None

        self.lib.sk_create.argtypes = [c_void_p, c_char_p, c_char_p, c_uint, c_char_p, c_uint, c_char_p]
        self.lib.sk_create.restype = c_int

        self.lib.sk_start_writing.argtypes = [c_void_p]
        self.lib.sk_start_writing.restype = c_int

        self.lib.sk_write_vector.argtypes = [c_void_p, c_uint64, c_char_p]
        self.lib.sk_write_vector.restype = c_int

        self.lib.sk_write_deleted.argtypes = [c_void_p, c_uint64]
        self.lib.sk_write_deleted.restype = c_int

        self.lib.sk_abort_writing.argtypes = [c_void_p]
        self.lib.sk_abort_writing.restype = c_int

        self.lib.sk_complete_writing.argtypes = [c_void_p]
        self.lib.sk_complete_writing.restype = c_int

        self.lib.sk_generate_test_metadata.argtypes = [c_void_p, c_char_p, c_uint64, c_uint64]
        self.lib.sk_generate_test_metadata.restype = c_int

        self.lib.sk_error.argtypes = [c_void_p]
        self.lib.sk_error.restype = c_int

        self.lib.sk_error_message.argtypes = [c_void_p]
        self.lib.sk_error_message.restype = c_char_p

    def close(self) -> None:
        if self.handle:
            self.lib.sk_release_handle(self.handle)
            self.handle = None

    def __enter__(self) -> "Sketch2Api":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _check(self, operation: str, rc: int) -> None:
        if rc == 0:
            return
        code = int(self.lib.sk_error(self.handle))
        message = self.lib.sk_error_message(self.handle)
        decoded = message.decode("utf-8", errors="replace") if message else ""
        raise Sketch2ApiError(f"{operation} failed (code={code}): {decoded}")

    def create_dataset(
        self,
        name: str,
        vectors: Iterable[tuple[int, str]],
        *,
        dim: int = 4,
        type_name: str = "f32",
        range_size: int = 1000,
        dist_func: str = "dot",
    ) -> None:
        self._check(
            "sk_create",
            self.lib.sk_create(
                self.handle,
                name.encode("utf-8"),
                None,
                c_uint(dim),
                type_name.encode("utf-8"),
                c_uint(range_size),
                dist_func.encode("utf-8"),
            ),
        )
        self._check("sk_start_writing", self.lib.sk_start_writing(self.handle))
        for item_id, value in vectors:
            self._check("sk_write_vector", self.lib.sk_write_vector(self.handle, c_uint64(item_id), value.encode("utf-8")))
        self._check("sk_complete_writing", self.lib.sk_complete_writing(self.handle))

    def start_writing(self) -> None:
        self._check("sk_start_writing", self.lib.sk_start_writing(self.handle))

    def write_vector(self, item_id: int, value: str) -> None:
        self._check("sk_write_vector", self.lib.sk_write_vector(self.handle, c_uint64(item_id), value.encode("utf-8")))

    def write_deleted(self, item_id: int) -> None:
        self._check("sk_write_deleted", self.lib.sk_write_deleted(self.handle, c_uint64(item_id)))

    def complete_writing(self) -> None:
        self._check("sk_complete_writing", self.lib.sk_complete_writing(self.handle))

    def abort_writing(self) -> None:
        self._check("sk_abort_writing", self.lib.sk_abort_writing(self.handle))

    def generate_test_metadata(
        self,
        file_path: str | Path,
        count: int,
        start_id: int = 0,
    ) -> None:
        self._check(
            "generate_test_metadata",
            self.lib.sk_generate_test_metadata(
                self.handle,
                str(file_path).encode("utf-8"),
                c_uint64(count),
                c_uint64(start_id),
            ),
        )


def fmt_f32_vector(values: list[float]) -> str:
    return ", ".join(f"{value:.6f}" for value in values)


def native_sequential_vector(item_id: int, dim: int = 4) -> list[float]:
    value = float(item_id) + 0.1
    return [value] * dim


def l2_distance_sq(a: list[float], b: list[float]) -> float:
    return sum((x - y) ** 2 for x, y in zip(a, b))


def metadata_values(item_id: int) -> tuple[int, int, int, str]:
    aaa = item_id % 2
    bbb = item_id % 5
    ccc = item_id % 10
    return aaa, bbb, ccc, f"aaa={aaa}, bbb={bbb}, ccc={ccc}"


def create_metadata_table(con) -> None:
    con.execute(
        """
        CREATE TABLE metadata (
            id BIGINT PRIMARY KEY,
            aaa BIGINT NOT NULL,
            bbb BIGINT NOT NULL,
            ccc BIGINT NOT NULL,
            text VARCHAR NOT NULL
        )
        """
    )


def load_metadata_csv_into_table(con, csv_path: Path) -> None:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    con.executemany(
        "INSERT INTO metadata(id, aaa, bbb, ccc, text) VALUES (?, ?, ?, ?, ?)",
        [
            (
                int(row["id"]),
                int(row["aaa"]),
                int(row["bbb"]),
                int(row["ccc"]),
                row["text"],
            )
            for row in rows
        ],
    )
