from __future__ import annotations

import ctypes
import importlib
import importlib.machinery
import importlib.util
import os
import shutil
import sys
from ctypes import c_char_p, c_int, c_uint, c_uint64, c_void_p
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def extension_path() -> Path:
    return repo_root() / "build" / "release" / "extension" / "sketch2" / "sketch2.duckdb_extension"


def import_duckdb_client():
    try:
        module = importlib.import_module("duckdb")
        if hasattr(module, "connect"):
            return module
    except ModuleNotFoundError:
        module = None

    venv_site_packages = sorted((repo_root() / ".pytest-venv" / "lib").glob("python*/site-packages"))
    for site_packages in venv_site_packages:
        site_packages_str = str(site_packages.resolve())
        if site_packages_str not in sys.path:
            sys.path.insert(0, site_packages_str)
        try:
            module = importlib.import_module("duckdb")
            if hasattr(module, "connect"):
                return module
        except ModuleNotFoundError:
            continue

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
            "Run `make python-test-deps` to install the repo's Python tutorial/test dependencies."
        )

    module = importlib.util.module_from_spec(spec)
    sys.modules["duckdb"] = module
    spec.loader.exec_module(module)
    if not hasattr(module, "connect"):
        raise ImportError("Resolved a duckdb module, but it does not expose duckdb.connect")
    return module


def libsketch2_path() -> Path:
    candidates = []

    env_root = os.environ.get("SKETCH2_ROOT")
    if env_root:
        candidates.append(Path(env_root) / "install" / "lib" / "libsketch2.so")

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

        self.lib.sk_complete_writing.argtypes = [c_void_p]
        self.lib.sk_complete_writing.restype = c_int

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
        vectors: list[tuple[int, str]],
        *,
        dim: int = 8,
        type_name: str = "f32",
        range_size: int = 10000,
        dist_func: str = "l2",
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


def parse_args_common(description: str):
    import argparse

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("dataset", help="Name of the dataset to create inside the tutorial Sketch2 database root")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("/tmp") / "sketch2duckdb_tutorial",
        help="Tutorial Sketch2 database root (default: /tmp/sketch2duckdb_tutorial)",
    )
    return parser.parse_args()


def reset_tutorial_root(root: Path) -> Path:
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def remove_dataset_dir(root: Path, dataset_name: str) -> None:
    dataset_dir = root / dataset_name
    if dataset_dir.exists():
        shutil.rmtree(dataset_dir)


def demo_vectors() -> list[tuple[int, str]]:
    values = {
        10: 0.00,
        20: 0.50,
        30: 1.00,
        40: 1.50,
        50: 2.00,
        60: 3.00,
        70: 6.00,
        80: 9.00,
    }
    return [(item_id, fmt_query_vector(value)) for item_id, value in values.items()]


def fmt_query_vector(value: float, dim: int = 8) -> str:
    return ", ".join([f"{value:.2f}"] * dim)


def metadata_rows() -> list[tuple[int, str, str, str]]:
    return [
        (10, "Intro to Vectors", "books", "alice"),
        (20, "Nearest Neighbor Notes", "blog", "bob"),
        (30, "Vector Search Handbook", "books", "carol"),
        (40, "Embeddings in Practice", "talks", "dave"),
        (50, "ANN Benchmarks", "books", "erin"),
        (60, "Similarity Metrics", "papers", "frank"),
        (70, "Large-Scale Retrieval", "papers", "grace"),
        (80, "Audio Similarity Primer", "music", "heidi"),
    ]


def create_demo_dataset(database_root: Path, dataset_name: str) -> None:
    remove_dataset_dir(database_root, dataset_name)
    with Sketch2Api(database_root) as api:
        api.create_dataset(dataset_name, demo_vectors())


def open_duckdb_connection():
    duckdb_client = import_duckdb_client()
    extension = extension_path()
    if not extension.exists():
        raise FileNotFoundError(f"Sketch2 extension binary not found at {extension}. Run `make` first.")
    con = duckdb_client.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{extension.as_posix()}'")
    return con


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def open_dataset_in_duckdb(con, database_root: Path, dataset_name: str) -> None:
    sql = f"PRAGMA sketch2_open({sql_string_literal(str(database_root))}, {sql_string_literal(dataset_name)})"
    print("Executing SQL:")
    print(sql)
    con.execute(sql)


def close_dataset_in_duckdb(con) -> None:
    sql = "PRAGMA sketch2_close"
    print("Executing SQL:")
    print(sql)
    con.execute(sql)


def create_metadata_table(con) -> None:
    create_sql = """
        CREATE TABLE metadata (
            id BIGINT PRIMARY KEY,
            title VARCHAR NOT NULL,
            category VARCHAR NOT NULL,
            author VARCHAR NOT NULL
        )
    """
    print("Executing SQL:")
    print(create_sql.strip())
    con.execute(create_sql)

    insert_sql = "INSERT INTO metadata(id, title, category, author) VALUES (?, ?, ?, ?)"
    print("Executing SQL:")
    print(insert_sql)
    con.executemany(insert_sql, metadata_rows())


def print_knn_rows(title: str, rows: list[tuple]) -> None:
    print("")
    print(title)
    if not rows:
        print("  (no rows)")
        return
    for rank, row in enumerate(rows, start=1):
        item_id, score = row
        print(f"  #{rank:02d} id={int(item_id):>3} score={float(score):.6f}")


def print_join_rows(title: str, rows: list[tuple]) -> None:
    print("")
    print(title)
    if not rows:
        print("  (no rows)")
        return
    for rank, row in enumerate(rows, start=1):
        item_id, item_title, category, author, score = row
        print(f"  #{rank:02d} id={int(item_id):>3} score={float(score):.6f}")
        print(f"      title   : {item_title}")
        print(f"      category: {category}")
        print(f"      author  : {author}")


def ensure_ids(rows: list[tuple], expected: list[int], label: str) -> None:
    actual = [int(row[0]) for row in rows]
    if actual != expected:
        raise RuntimeError(f"{label}: expected ids {expected}, got {actual}")
