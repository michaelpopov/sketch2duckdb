#!/usr/bin/env python3
"""Tutorial showing how to join DuckDB KNN results with metadata tables."""

from __future__ import annotations

from duckdb_tutorial_utils import (
    create_demo_dataset,
    create_metadata_table,
    ensure_ids,
    fmt_query_vector,
    open_dataset_in_duckdb,
    open_duckdb_connection,
    parse_args_common,
    print_join_rows,
    reset_tutorial_root,
)


def main() -> None:
    args = parse_args_common("Run DuckDB queries that join Sketch2 KNN results with metadata.")
    database_root = reset_tutorial_root(args.root)
    dataset_name = args.dataset

    create_demo_dataset(database_root, dataset_name)
    print(f"Prepared Sketch2 dataset '{dataset_name}' in {database_root}")

    con = open_duckdb_connection()
    try:
        open_dataset_in_duckdb(con, database_root, dataset_name)
        create_metadata_table(con)

        join_sql = """
            SELECT m.id, m.title, m.category, m.author, n.score
            FROM sketch2_knn(?, ?, NULL) AS n
            JOIN metadata AS m ON m.id = n.id
            ORDER BY n.score, n.id
        """
        print("Executing SQL:")
        print(join_sql.strip())
        rows = con.execute(join_sql, [fmt_query_vector(1.10), 5]).fetchall()
        print_join_rows("Joined KNN rows (no metadata filter):", rows)
        ensure_ids(rows, [30, 40, 20, 50, 10], "DuckDB KNN join without metadata filter")

        filtered_join_sql = """
            SELECT m.id, m.title, m.category, m.author, n.score
            FROM sketch2_knn(?, ?, NULL) AS n
            JOIN metadata AS m ON m.id = n.id
            WHERE m.category = ?
            ORDER BY n.score, n.id
        """
        print("Executing SQL:")
        print(filtered_join_sql.strip())
        rows = con.execute(filtered_join_sql, [fmt_query_vector(1.10), 8, "books"]).fetchall()
        print_join_rows("Joined KNN rows (metadata filter category='books'):", rows)
        ensure_ids(rows, [30, 50, 10], "DuckDB KNN join with metadata filter")

        print("")
        print("Completed validating DuckDB joins between KNN results and metadata.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
