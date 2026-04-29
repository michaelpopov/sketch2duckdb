PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
PYTEST_VENV := $(PROJ_DIR).pytest-venv
PYTEST_PYTHON := $(PYTEST_VENV)/bin/python3
PYTEST_STAMP := $(PYTEST_VENV)/.deps-installed
SQL_FIXTURE_ROOT := $(PROJ_DIR)test/generated/sketch2_sql_fixture
SQL_FIXTURE_STAMP := $(SQL_FIXTURE_ROOT)/.generated

# Configuration of extension
EXT_NAME=sketch2
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: python-test-deps pytest generate-sql-fixture tut

$(PYTEST_PYTHON):
	python3 -m venv $(PYTEST_VENV)

$(PYTEST_STAMP): $(PYTEST_PYTHON) requirements-dev.txt duckdb/CMakeLists.txt
	@$(PYTEST_PYTHON) -m pip install -r requirements-dev.txt
	@DUCKDB_GIT_DESCRIBE=`git -C duckdb describe --tags --long --match 'v[0-9]*'`; \
	DUCKDB_PY_VERSION=`printf '%s\n' "$$DUCKDB_GIT_DESCRIBE" | sed -E 's/^v([0-9]+\.[0-9]+\.[0-9]+)-0-g[0-9a-f]+$$/\1/'`; \
	if [ "$$DUCKDB_PY_VERSION" = "" ] || [ "$$DUCKDB_PY_VERSION" = "$$DUCKDB_GIT_DESCRIBE" ]; then \
		echo "Unable to determine a matching PyPI DuckDB version for Python tests."; \
		echo "The checked-out duckdb tree must be exactly on a release tag so the loadable extension and Python client use the same version."; \
		echo "Expected git describe output like: vX.Y.Z-0-g<hash>"; \
		echo "Actual git describe output: $$DUCKDB_GIT_DESCRIBE"; \
		exit 1; \
	fi; \
	echo "Installing duckdb==$$DUCKDB_PY_VERSION into $(PYTEST_VENV)"; \
	$(PYTEST_PYTHON) -m pip install "duckdb==$$DUCKDB_PY_VERSION"
	@touch $(PYTEST_STAMP)

python-test-deps: $(PYTEST_STAMP)

pytest: $(PYTEST_STAMP)
	$(PYTEST_PYTHON) -m pytest test/python -q

$(SQL_FIXTURE_STAMP):
	python3 test/generate_sql_fixture.py "$(SQL_FIXTURE_ROOT)"
	@touch $(SQL_FIXTURE_STAMP)

generate-sql-fixture: $(SQL_FIXTURE_STAMP)

tut: test $(PYTEST_STAMP)
	$(PYTEST_PYTHON) tutorial/run_all.py

test_release_internal: $(SQL_FIXTURE_STAMP)
test_debug_internal: $(SQL_FIXTURE_STAMP)
test_reldebug_internal: $(SQL_FIXTURE_STAMP)
