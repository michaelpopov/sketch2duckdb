#!/usr/bin/env python3
"""Tutorial showing how to query Sketch2 KNN results from DuckDB SQL."""

from __future__ import annotations

from duckdb_tutorial_utils import (
    create_demo_dataset,
    ensure_ids,
    fmt_query_vector,
    open_dataset_in_duckdb,
    open_duckdb_connection,
    parse_args_common,
    print_knn_rows,
    reset_tutorial_root,
)


def main() -> None:
    args = parse_args_common("Run a basic DuckDB KNN query against a Sketch2 dataset.")
    database_root = reset_tutorial_root(args.root)
    dataset_name = args.dataset

    create_demo_dataset(database_root, dataset_name)
    print(f"Prepared Sketch2 dataset '{dataset_name}' in {database_root}")

    con = open_duckdb_connection()
    try:
        open_dataset_in_duckdb(con, database_root, dataset_name)

        version_sql = "SELECT sketch2_version(), sketch2_dataset()"
        print("Executing SQL:")
        print(version_sql)
        version_row = con.execute(version_sql).fetchone()
        print(f"Sketch2 version: {version_row[0]}")
        print(f"Opened dataset : {version_row[1]}")

        knn_sql = """
            SELECT id, score
            FROM sketch2_knn(?, ?, NULL)
            ORDER BY score, id
        """
        print("Executing SQL:")
        print(knn_sql.strip())
        rows = con.execute(knn_sql, [fmt_query_vector(1.10), 5]).fetchall()
        print_knn_rows("Nearest neighbors for query vector 1.10 x 8:", rows)
        ensure_ids(rows, [30, 40, 20, 50, 10], "Basic DuckDB KNN query")

        print("")
        print("Completed validating basic DuckDB KNN querying.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
