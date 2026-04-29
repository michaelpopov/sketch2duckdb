# Sketch2 DuckDB Extension

This repository contains a DuckDB extension that lets DuckDB query existing
Sketch2 datasets.

Sketch2 is intentionally focused on vector storage and vector search, while
DuckDB is strong at SQL, joins, analytics, and metadata processing. This
extension connects those two pieces: Sketch2 does the nearest-neighbor work,
and DuckDB gives you a convenient relational surface around it.

This repository started from
https://github.com/duckdb/extension-template and then evolved into a
Sketch2-specific extension with:

- session-scoped dataset opening
- KNN search from DuckDB
- named allow-list pushdown through `sketch2_bitset_filter`
- query vectors accepted as Sketch2 text or DuckDB float arrays

## Why This Extension Exists

Sketch2 documentation describes Sketch2 as a vector storage and compute engine,
not a general-purpose database. That design is deliberate: Sketch2 optimizes
how vectors are stored, scanned, and scored on real hardware, while a host
database is expected to own relational querying, joins, metadata filters, and
result shaping.

DuckDB is a very good fit for that host-database role:

- you can keep metadata in normal DuckDB tables
- you can query Sketch2 from SQL instead of writing custom C or Python code
- you can join KNN results with structured data
- you can generate candidate id sets in DuckDB and push them down into Sketch2

In short, this extension exists so users can treat Sketch2 as a specialized
vector engine inside DuckDB workflows.

## What The Extension Provides

Today this extension is a query-side integration. It does not create datasets
or write vectors from DuckDB. Instead, it assumes the Sketch2 dataset already
exists and exposes the read/query path inside DuckDB.

The current SQL surface is:

- `sketch2_version()`:
  returns the Sketch2 library version
- `sketch2_dataset()`:
  returns the currently opened dataset name for the current connection
- `PRAGMA sketch2_open(database_path, dataset_name)`:
  opens a Sketch2 dataset for the current DuckDB connection
- `PRAGMA sketch2_close`:
  closes the currently opened Sketch2 dataset for the current DuckDB connection
- `sketch2_knn(query_vector, k, bitset_filter_name)`:
  returns top-k nearest neighbors as `(id, score)`
- `sketch2_bitset_filter(id, name)`:
  aggregate that turns a DuckDB result set of ids into a named Sketch2 allow-list
- `sketch2_bitset_load(name)`:
  validates a named Sketch2 allow-list
- `sketch2_bitset_drop(name)`:
  removes a named Sketch2 allow-list
- `sketch2_bitset_cache_remove(name)`:
  removes one named bitset filter from Sketch2's in-process cache
- `sketch2_bitset_cache_clear()`:
  clears Sketch2's in-process bitset filter cache

## How It Works

The integration follows the same separation of responsibilities that Sketch2
already uses for SQLite:

1. Sketch2 owns the dataset files, read path, and scoring.
2. DuckDB owns SQL execution, joins, metadata filters, and application-facing
   query composition.
3. The extension keeps a Sketch2 handle in DuckDB client context state, so the
   opened dataset belongs to the current DuckDB connection.
4. `sketch2_knn` calls into the Sketch2 C API and returns rows back to DuckDB
   as a table function.
5. `sketch2_bitset_filter` builds a named Sketch2 bitset filter from ids
   produced by a DuckDB query. Sketch2 owns the persisted filter, and
   `sketch2_knn` later loads it by name.

Important behavioral details:

- one DuckDB connection tracks one active opened Sketch2 dataset at a time
- opening a new dataset replaces the previous handle in that connection
- bitset filters are named and owned by Sketch2
- named filters can be reused across DuckDB queries
- `sketch2_bitset_load(name)` validates a named filter explicitly
- `sketch2_bitset_drop(name)` removes a named filter when it is no longer needed
- `sketch2_bitset_cache_remove(name)` evicts one named filter from Sketch2's cache
- `sketch2_bitset_cache_clear()` evicts all named filters from Sketch2's cache
- this extension is read/query oriented; dataset creation and mutation still
  happen through Sketch2 itself

## Building

### Managing dependencies

This extension depends on an external Sketch2 build. Before configuring or
building, set `SKETCH2_ROOT` to the root of your Sketch2 repository:

```sh
export SKETCH2_ROOT=/path/to/sketch2
```

During the build, this project uses:

- `"$SKETCH2_ROOT/install/include"` as an additional compiler include path
- `"$SKETCH2_ROOT/install/lib"` to find `libsketch2.so` for linking

That means Sketch2 should already be built and installed into the `install`
subdirectory under `SKETCH2_ROOT`.

### Build steps

Build the extension with:

```sh
make
```

The main binaries that will be built are:

```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/sketch2/sketch2.duckdb_extension
```

- `duckdb` is the DuckDB shell with the extension code already linked in
- `unittest` is DuckDB's test runner with the extension linked in
- `sketch2.duckdb_extension` is the loadable extension artifact

## Using The Extension In DuckDB

If you use the shell built by this repository, start:

```sh
./build/release/duckdb
```

If you use another DuckDB client, load the built extension artifact:

```sql
LOAD '/absolute/path/to/build/release/extension/sketch2/sketch2.duckdb_extension';
```

The extension queries an existing Sketch2 dataset. In Sketch2 terminology:

- `database_path` is the Sketch2 database root directory
- `dataset_name` is the dataset to open inside that root

## SQL API Reference

### `sketch2_version()`

Scalar function that returns the Sketch2 library version.

Example:

```sql
SELECT sketch2_version();
```

### `sketch2_dataset()`

Scalar function that returns the currently opened dataset name.

Errors:

- raises an error if no dataset is open on the current connection

Example:

```sql
SELECT sketch2_dataset();
```

### `PRAGMA sketch2_open(database_path, dataset_name)`

PRAGMA that opens a Sketch2 dataset for the current DuckDB connection.

```sql
PRAGMA sketch2_open('/mnt/nvme/sketch2/db', 'items');
```

Notes:

- arguments must be non-`NULL`
- the dataset must already exist
- `sketch2_knn` requires `PRAGMA sketch2_open(...)` to be called first

### `PRAGMA sketch2_close`

PRAGMA that closes the currently opened Sketch2 dataset for the current
DuckDB connection. If no dataset is open, this is a no-op.

Example:

```sql
PRAGMA sketch2_close;
```

### `sketch2_knn(query_vector, k, bitset_filter_name)`

Table function that returns nearest neighbors with schema:

```text
(id UBIGINT, score DOUBLE)
```

Arguments:

- `query_vector`: the query vector
- `k`: number of neighbors to return, must be `> 0` and `<= 1,000,000`
- `bitset_filter_name`: optional name of a filter created by
  `sketch2_bitset_filter(id, name)`; pass `NULL` for no filter

Supported query-vector formats:

- Sketch2 text format as `VARCHAR`
- DuckDB `FLOAT[]`
- DuckDB `DOUBLE[]`
- DuckDB float/double `ARRAY`

For `VARCHAR`, the extension forwards the string to Sketch2's normal parser, so
the accepted text forms are whatever Sketch2 supports for the dataset, such as
comma-delimited values like `'1.0, 2.0, 3.0, 4.0'`.

Examples:

```sql
SELECT *
FROM sketch2_knn('1.0, 2.0, 3.0, 4.0', 5, NULL);

SELECT *
FROM sketch2_knn([1.0, 2.0, 3.0, 4.0]::FLOAT[], 5, NULL);

SELECT *
FROM sketch2_knn([1.0, 2.0, 3.0, 4.0]::FLOAT[], 5, 'books_filter');
```

Score ordering depends on the Sketch2 dataset metric:

- for `l2` and `cos`, smaller scores are better, so use `ORDER BY score ASC`
- for `dot`, larger scores are better, so use `ORDER BY score DESC`

### `sketch2_bitset_filter(id, name)`

Aggregate function that turns a set of ids into a named Sketch2 allow-list and
echoes the provided filter name on success.

Requirements and limits:

- input ids must be non-negative `BIGINT`
- `name` must be a constant, non-`NULL`, non-empty `VARCHAR`
- rows with `NULL` ids are ignored
- at most 1,000,000 ids are allowed per aggregate group
- the named filter is persisted by Sketch2 and can be reused by later queries

Example:

```sql
SELECT sketch2_bitset_filter(id, 'books_filter')
FROM metadata
WHERE category = 'books';
```

DuckDB does not require ids passed to `sketch2_bitset_filter(id, name)` to be
pre-sorted. The aggregate collects ids during execution and hands them to
Sketch2's builder at finalize time, and Sketch2 owns the resulting named
filter.

### `sketch2_bitset_load(name)`

Scalar function that validates a named Sketch2 allow-list. `sketch2_knn(...,
bitset_filter_name)` can load filters lazily by name, so this function is mainly
useful as an explicit preflight step.

Arguments:

- `name`: constant, non-`NULL`, non-empty name of the filter to load

Returns the filter name on success. Missing, invalid, or malformed filters raise
a DuckDB error.

Example:

```sql
SELECT sketch2_bitset_load('books_filter');
```

### `sketch2_bitset_drop(name)`

Scalar function that removes a named Sketch2 allow-list.

Arguments:

- `name`: constant, non-`NULL`, non-empty name of the filter to remove

Returns `1` when a filter file was removed and `0` when there was no matching
filter.

Example:

```sql
SELECT sketch2_bitset_drop('books_filter');
```

### `sketch2_bitset_cache_remove(name)`

Scalar function that evicts one named Sketch2 allow-list from Sketch2's
in-process cache. The on-disk filter file is not removed.

Arguments:

- `name`: constant, non-`NULL`, non-empty name of the cached filter to remove

Returns `1` when a cache entry was removed and `0` when there was no matching
cached entry.

Example:

```sql
SELECT sketch2_bitset_cache_remove('books_filter');
```

### `sketch2_bitset_cache_clear()`

Scalar function that clears Sketch2's in-process bitset-filter cache.

Returns `true` on success.

Example:

```sql
SELECT sketch2_bitset_cache_clear();
```

## Query Examples

### 1. Open a dataset and inspect state

```sql
PRAGMA sketch2_open('/mnt/nvme/sketch2/db', 'items');

SELECT sketch2_version() AS sketch2_version;
SELECT sketch2_dataset() AS opened_dataset;
```

### 2. Basic KNN query for an `l2` or `cos` dataset

```sql
SELECT id, score
FROM sketch2_knn(
    [7.4, 7.4, 7.4, 7.4]::FLOAT[],
    5,
    NULL
)
ORDER BY score, id;
```

### 3. Basic KNN query for a `dot` dataset

```sql
SELECT id, score
FROM sketch2_knn(
    '1.0, 1.0, 1.0, 1.0',
    5,
    NULL
)
ORDER BY score DESC, id;
```

`dot` uses similarity-style scoring rather than distance-style scoring, so
higher scores are better matches. That is why this query orders by `score DESC`
instead of ascending order.

### 4. Join KNN results with DuckDB metadata

```sql
CREATE TABLE metadata (
    id BIGINT PRIMARY KEY,
    title VARCHAR,
    category VARCHAR
);

SELECT n.id, n.score, m.title, m.category
FROM sketch2_knn([7.4, 7.4, 7.4, 7.4]::FLOAT[], 5, NULL) AS n
JOIN metadata AS m ON m.id = n.id
ORDER BY n.score, n.id;
```

This is the main value of the integration: Sketch2 handles the vector search,
while DuckDB handles metadata joins and the rest of the SQL pipeline.

### 5. Push a metadata-derived allow-list into Sketch2

The name-based filter workflow is:

1. build a named filter from DuckDB rows
2. pass that filter name into `sketch2_knn`

Step 1:

```sql
SELECT sketch2_bitset_filter(id, 'books_filter') AS filter_name
FROM metadata
WHERE category = 'books';
```

Step 2:

```sql
SELECT n.id, n.score, m.title
FROM sketch2_knn([7.4, 7.4, 7.4, 7.4]::FLOAT[], 5, 'books_filter') AS n
JOIN metadata AS m ON m.id = n.id
ORDER BY n.score, n.id;
```

For example, from Python:

```python
import duckdb

con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
con.execute(
    "LOAD '/absolute/path/to/build/release/extension/sketch2/sketch2.duckdb_extension'"
)
con.execute("PRAGMA sketch2_open(?, ?)", ["/mnt/nvme/sketch2/db", "items"])

filter_name = con.execute(
    """
    SELECT sketch2_bitset_filter(id, ?)
    FROM metadata
    WHERE category = 'books'
    """,
    ["books_filter"],
).fetchone()[0]

rows = con.execute(
    """
    SELECT n.id, n.score, m.title
    FROM sketch2_knn(?, ?, ?) AS n
    JOIN metadata AS m ON m.id = n.id
    ORDER BY n.score, n.id
    """,
    [[7.4, 7.4, 7.4, 7.4], 5, filter_name],
).fetchall()
```

This pattern is useful when DuckDB can cheaply derive a candidate id set from
relational predicates and Sketch2 should search only within that subset.

Named filters can be dropped when they are no longer needed:

```sql
SELECT sketch2_bitset_drop('books_filter');
```

## What This Extension Does Not Do

To avoid confusion, the current extension does not yet expose:

- dataset creation from DuckDB
- staged writes, deletes, or merges from DuckDB
- dataset management DDL
- automatic one-statement filter pushdown equivalent to Sketch2's SQLite
  virtual-table hidden columns

Those capabilities still live in Sketch2's own C/Python APIs and other
integrations.

## Running The Tests

The primary extension tests are SQLLogicTests in `./test/sql`:

```sh
make test
```

Those test targets generate the SQL fixture dataset under `test/generated/`
before running the SQL tests.

For Python integration tests, first install the repo-local test dependencies:

```sh
make python-test-deps
```

Then run:

```sh
make pytest
```

## Installing Deployed Binaries

To install your extension binaries from S3, DuckDB must be launched with
`allow_unsigned_extensions` enabled. How to do that depends on the client.

CLI:

```sh
duckdb -unsigned
```

Python:

```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions': 'true'})
```

NodeJS:

```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Then set the custom extension repository:

```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```

After that you can install and load the extension with normal DuckDB commands:

```sql
INSTALL sketch2;
LOAD sketch2;
```
