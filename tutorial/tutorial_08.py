#!/usr/bin/env python3
"""Tutorial showing reuse and cleanup of persisted Sketch2 bitset filters in DuckDB."""

from __future__ import annotations

from duckdb_tutorial_utils import (
    create_demo_dataset,
    create_metadata_table,
    ensure_ids,
    fmt_query_vector,
    open_dataset_in_duckdb,
    open_duckdb_connection,
    parse_args_common,
    print_knn_rows,
    reset_tutorial_root,
)


def main() -> None:
    args = parse_args_common("Run DuckDB queries that reuse a persisted Sketch2 bitset filter.")
    database_root = reset_tutorial_root(args.root)
    dataset_name = args.dataset
    filter_name = f"{dataset_name}_books_filter"

    create_demo_dataset(database_root, dataset_name)
    print(f"Prepared Sketch2 dataset '{dataset_name}' in {database_root}")

    con = open_duckdb_connection()
    try:
        open_dataset_in_duckdb(con, database_root, dataset_name)
        create_metadata_table(con)

        build_filter_sql = """
            SELECT sketch2_bitset_filter(id, ?)
            FROM metadata
            WHERE category = ?
        """
        print("Executing SQL:")
        print(build_filter_sql.strip())
        created_name = con.execute(build_filter_sql, [filter_name, "books"]).fetchone()[0]
        print(f"Persisted named Sketch2 bitset filter: {created_name}")

        load_filter_sql = "SELECT sketch2_bitset_load(?)"
        print("Executing SQL:")
        print(load_filter_sql)
        loaded_name = con.execute(load_filter_sql, [filter_name]).fetchone()[0]
        if loaded_name != filter_name:
            raise RuntimeError(f"Expected sketch2_bitset_load to return {filter_name}, got {loaded_name}")

        knn_sql = """
            SELECT id, score
            FROM sketch2_knn(?, ?, ?)
            ORDER BY score, id
        """
        print("Executing SQL:")
        print(knn_sql.strip())
        first_rows = con.execute(knn_sql, [fmt_query_vector(1.10), 3, filter_name]).fetchall()
        print_knn_rows("First KNN query using persisted named filter:", first_rows)
        ensure_ids(first_rows, [30, 50, 10], "First reused bitset filter query")

        second_rows = con.execute(knn_sql, [fmt_query_vector(1.60), 3, filter_name]).fetchall()
        print_knn_rows("Second KNN query reusing the same named filter:", second_rows)
        ensure_ids(second_rows, [50, 30, 10], "Second reused bitset filter query")

        cache_remove_sql = "SELECT sketch2_bitset_cache_remove(?)"
        print("Executing SQL:")
        print(cache_remove_sql)
        removed = con.execute(cache_remove_sql, [filter_name]).fetchone()[0]
        if removed != 1:
            raise RuntimeError(f"Expected sketch2_bitset_cache_remove to return 1, got {removed}")

        third_rows = con.execute(knn_sql, [fmt_query_vector(1.10), 3, filter_name]).fetchall()
        print_knn_rows("KNN query after cache eviction reloads the same named filter:", third_rows)
        ensure_ids(third_rows, [30, 50, 10], "Bitset filter query after cache eviction")

        drop_sql = "SELECT sketch2_bitset_drop(?)"
        print("Executing SQL:")
        print(drop_sql)
        dropped = con.execute(drop_sql, [filter_name]).fetchone()[0]
        if dropped != 1:
            raise RuntimeError(f"Expected sketch2_bitset_drop to return 1, got {dropped}")

        dropped_again = con.execute(drop_sql, [filter_name]).fetchone()[0]
        if dropped_again != 0:
            raise RuntimeError(f"Expected second sketch2_bitset_drop to return 0, got {dropped_again}")

        print("")
        print("Completed validating persisted named filter reuse and cleanup.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
