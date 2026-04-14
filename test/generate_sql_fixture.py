from __future__ import annotations

import ctypes
import os
import shutil
import sys
from ctypes import c_char_p, c_int, c_uint, c_uint64, c_void_p
from pathlib import Path


def libsketch2_path(repo_root: Path) -> Path:
    candidates = []

    env_root = os.environ.get("SKETCH2_ROOT")
    if env_root:
        candidates.append(Path(env_root) / "install" / "lib" / "libsketch2.so")

    candidates.append(repo_root.parent / "sketch2" / "install" / "lib" / "libsketch2.so")

    for candidate in candidates:
        if candidate.exists():
            return candidate

    searched = ", ".join(str(candidate) for candidate in candidates)
    raise FileNotFoundError(f"libsketch2.so not found. Checked: {searched}")


class Sketch2FixtureBuilder:
    def __init__(self, dataset_root: Path, repo_root: Path):
        self.dataset_root = dataset_root
        self.lib = ctypes.CDLL(str(libsketch2_path(repo_root)))
        self._configure()
        self.handle = self.lib.sk_new_handle(str(self.dataset_root).encode("utf-8"))
        if not self.handle:
            raise RuntimeError("sk_new_handle returned NULL")

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

    def _check(self, operation: str, rc: int) -> None:
        if rc == 0:
            return
        code = int(self.lib.sk_error(self.handle))
        message = self.lib.sk_error_message(self.handle)
        decoded = message.decode("utf-8", errors="replace") if message else ""
        raise RuntimeError(f"{operation} failed (code={code}): {decoded}")

    def create_fixture(self) -> None:
        self._check(
            "sk_create",
            self.lib.sk_create(
                self.handle,
                b"items",
                None,
                c_uint(4),
                b"f32",
                c_uint(1000),
                b"dot",
            ),
        )
        self._check("sk_start_writing", self.lib.sk_start_writing(self.handle))
        for item_id, value in (
            (10, "0.0, 0.0, 0.0, 0.0"),
            (20, "5.0, 5.0, 5.0, 5.0"),
            (30, "10.0, 10.0, 10.0, 10.0"),
        ):
            self._check("sk_write_vector", self.lib.sk_write_vector(self.handle, c_uint64(item_id), value.encode("utf-8")))
        self._check("sk_complete_writing", self.lib.sk_complete_writing(self.handle))


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: python3 test/generate_sql_fixture.py <fixture-root>", file=sys.stderr)
        return 1

    fixture_root = Path(sys.argv[1]).resolve()
    repo_root = Path(__file__).resolve().parents[1]

    if fixture_root.exists():
        shutil.rmtree(fixture_root)
    fixture_root.mkdir(parents=True, exist_ok=True)

    builder = Sketch2FixtureBuilder(fixture_root, repo_root)
    try:
        builder.create_fixture()
    finally:
        builder.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
