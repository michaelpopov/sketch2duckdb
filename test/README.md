# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```

Those test targets generate any SQL fixture datasets under `test/generated/`
before running the sqllogictests.

For Python integration tests under `test/python`, first install the repo's
Python test dependencies:

```bash
make python-test-deps
```

Then run:

```bash
make pytest
```
