# Python Integration Tests

These tests exercise the built Sketch2 DuckDB extension through the Python
`duckdb` client while creating real Sketch2 datasets through `libsketch2.so`.

Expected prerequisites:

- `python3`
- a built extension at `build/release/extension/sketch2/sketch2.duckdb_extension`
- a built Sketch2 shared library at `$SKETCH2_ROOT/install/lib/libsketch2.so`
  or `../sketch2/install/lib/libsketch2.so`

Install the Python test dependencies into the repo-local venv:

```sh
make python-test-deps
```

Then run the tests with:

```sh
make pytest
```
