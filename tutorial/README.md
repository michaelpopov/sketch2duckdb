# Sketch2 DuckDB Tutorials

This directory mirrors the SQL-focused part of the upstream Sketch2 tutorial
experience, but uses this DuckDB extension instead of the SQLite integration.

These scripts are intentionally ordered from simpler to more advanced DuckDB
workflows:

- `tutorial_05.py`: run a basic `sketch2_knn(...)` query from DuckDB SQL
- `tutorial_06.py`: join KNN results with relational metadata in DuckDB
- `tutorial_07.py`: push metadata-derived filters into Sketch2 KNN using
  `sketch2_bitset_filter(...)`
- `tutorial_08.py`: reuse persisted named filters with
  `sketch2_bitset_load(...)`, cache eviction, and `sketch2_bitset_drop(...)`

Unlike the upstream Sketch2 tutorial directory, this one does not replicate
the non-database examples. It focuses only on scripts that exercise SQL and
database integration.

## Prerequisites

Build the extension first:

```sh
make
```

The tutorial scripts also need:

- the Python `duckdb` package
- a built `libsketch2.so`

If `SKETCH2_ROOT` is set, the scripts use:

```text
$SKETCH2_ROOT/install/lib/libsketch2.so
```

Otherwise they fall back to the sibling repository path:

```text
../sketch2/install/lib/libsketch2.so
```

## Running One Tutorial

Each tutorial script creates its own dataset inside a Sketch2 database root and
then queries it through DuckDB.

Example:

```sh
python3 tutorial/tutorial_05.py demo_dataset
```

You can also choose a different root directory:

```sh
python3 tutorial/tutorial_07.py --root /tmp/sketch2duckdb_demo demo_dataset
```

## Running All Tutorials

To run the whole sequence:

```sh
python3 tutorial/run_all.py
```

By default the runner uses:

```text
/tmp/sketch2duckdb_tutorial
```

You can override that root if needed:

```sh
python3 tutorial/run_all.py --root /tmp/another_tutorial_root
```

## What Each Tutorial Demonstrates

### `tutorial_05.py`

- loads the built DuckDB extension
- opens a Sketch2 dataset with `PRAGMA sketch2_open(...)`
- runs a basic nearest-neighbor query with `sketch2_knn(...)`
- confirms the expected ordering of ids

### `tutorial_06.py`

- creates a normal DuckDB `metadata` table
- joins `sketch2_knn(...)` results with relational metadata
- applies a metadata filter after the join

### `tutorial_07.py`

- compares post-filtering versus pushed-down filtering
- builds a named filter with `sketch2_bitset_filter(id, name)`
- passes that filter name into `sketch2_knn(...)`
- shows that pushdown can change which neighbors are returned for the same `k`

### `tutorial_08.py`

- persists a named Sketch2 filter once
- validates it with `sketch2_bitset_load(name)`
- reuses it across multiple KNN queries
- evicts it from the process cache with `sketch2_bitset_cache_remove(name)`
- removes it from storage with `sketch2_bitset_drop(name)`
