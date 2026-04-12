# Sketch2

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, Sketch2, allow you to run KNN search on Sketch2 datasets.


## Building
### Managing dependencies
This extension depends on an external Sketch2 build. Before configuring or building, set `SKETCH2_ROOT` to the root of your Sketch2 repository:
```shell
export SKETCH2_ROOT=/path/to/sketch2
```
During the build, this project uses:
- `"$SKETCH2_ROOT/install/include"` as an additional compiler include path
- `"$SKETCH2_ROOT/install/lib"` to find `libsketch2.so` for linking

That means Sketch2 should already be built and installed into the `install` subdirectory under `SKETCH2_ROOT`.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/sketch2/sketch2.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `sketch2.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. The template contains a single scalar function `sketch2()` that takes a string arguments and returns a string:
```
D select sketch2('Jane') as result;
┌───────────────┐
│    result     │
│    varchar    │
├───────────────┤
│ Sketch2 Jane 🐥 │
└───────────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL sketch2;
LOAD sketch2;
```
