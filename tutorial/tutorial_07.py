#!/usr/bin/env python3
"""Tutorial showing metadata-filter pushdown into Sketch2 KNN from DuckDB."""

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
    args = parse_args_common("Run DuckDB queries that push metadata filters into Sketch2 KNN.")
    database_root = reset_tutorial_root(args.root)
    dataset_name = args.dataset
    filter_name = f"{dataset_name}_books_filter"

    create_demo_dataset(database_root, dataset_name)
    print(f"Prepared Sketch2 dataset '{dataset_name}' in {database_root}")

    con = open_duckdb_connection()
    try:
        open_dataset_in_duckdb(con, database_root, dataset_name)
        create_metadata_table(con)

        post_filter_sql = """
            SELECT m.id, m.title, m.category, m.author, n.score
            FROM sketch2_knn(?, ?, NULL) AS n
            JOIN metadata AS m ON m.id = n.id
            WHERE m.category = ?
            ORDER BY n.score, n.id
        """
        print("Executing SQL:")
        print(post_filter_sql.strip())
        post_filtered_rows = con.execute(post_filter_sql, [fmt_query_vector(1.10), 3, "books"]).fetchall()
        print_join_rows("Post-filtered rows after plain KNN (k=3):", post_filtered_rows)
        ensure_ids(post_filtered_rows, [30], "DuckDB post-filter example")

        build_filter_sql = """
            SELECT sketch2_bitset_filter(id, ?)
            FROM metadata
            WHERE category = ?
        """
        print("Executing SQL:")
        print(build_filter_sql.strip())
        created_name = con.execute(build_filter_sql, [filter_name, "books"]).fetchone()[0]
        print(f"Created named Sketch2 bitset filter: {created_name}")

        pushdown_sql = """
            SELECT m.id, m.title, m.category, m.author, n.score
            FROM sketch2_knn(?, ?, ?) AS n
            JOIN metadata AS m ON m.id = n.id
            ORDER BY n.score, n.id
        """
        print("Executing SQL:")
        print(pushdown_sql.strip())
        pushed_down_rows = con.execute(pushdown_sql, [fmt_query_vector(1.10), 3, filter_name]).fetchall()
        print_join_rows("Pushed-down rows using sketch2_bitset_filter (k=3):", pushed_down_rows)
        ensure_ids(pushed_down_rows, [30, 50, 10], "DuckDB pushed-down filter example")

        print("")
        print("Completed validating metadata-filter pushdown into Sketch2 KNN.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
