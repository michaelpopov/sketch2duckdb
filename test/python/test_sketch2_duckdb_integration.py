from __future__ import annotations

from pathlib import Path

from sketch2_test_utils import (
    Sketch2Api,
    create_metadata_table,
    fmt_f32_vector,
    l2_distance_sq,
    load_metadata_csv_into_table,
    metadata_values,
    native_sequential_vector,
)


DIM = 4
COUNT = 20
START_ID = 0

def query_vector(query_value: float) -> str:
    return fmt_f32_vector([query_value] * DIM)


def expected_knn_rows(
    query_value: float,
    k: int,
    *,
    allowed_ids: set[int] | None = None,
    start_id: int = START_ID,
    count: int = COUNT,
) -> list[tuple[int, float]]:
    query = [query_value] * DIM
    rows: list[tuple[int, float]] = []
    for item_id in range(start_id, start_id + count):
        if allowed_ids is not None and item_id not in allowed_ids:
            continue
        score = l2_distance_sq(query, native_sequential_vector(item_id, DIM))
        rows.append((item_id, score))
    rows.sort(key=lambda row: (row[1], row[0]))
    return rows[:k]


def create_sequential_dataset(root: Path, dataset_name: str, *, count: int = COUNT, start_id: int = START_ID) -> None:
    vectors = [
        (item_id, fmt_f32_vector(native_sequential_vector(item_id, DIM)))
        for item_id in range(start_id, start_id + count)
    ]
    with Sketch2Api(root) as api:
        api.create_dataset(dataset_name, vectors, dim=DIM, type_name="f32", range_size=1000, dist_func="l2")


def load_metadata_table(con, root: Path, *, count: int = COUNT, start_id: int = START_ID) -> None:
    metadata_csv_path = root / "metadata.csv"
    with Sketch2Api(root) as api:
        api.generate_test_metadata(metadata_csv_path, count=count, start_id=start_id)
    create_metadata_table(con)
    load_metadata_csv_into_table(con, metadata_csv_path)


def bitset_filter_ref_for_predicate(con, predicate_sql: str) -> int:
    return con.execute(
        f"""
        SELECT bitset_filter(id)
        FROM (SELECT id FROM metadata WHERE {predicate_sql} ORDER BY id)
        """
    ).fetchone()[0]


def assert_knn_rows_equal(actual: list[tuple[int, float]], expected: list[tuple[int, float]]) -> None:
    assert [item_id for item_id, _ in actual] == [item_id for item_id, _ in expected]
    assert len(actual) == len(expected)
    for (_, actual_score), (_, expected_score) in zip(actual, expected):
        assert abs(actual_score - expected_score) < 1e-4


def assert_join_rows_match_ids(actual: list[tuple[int, float, int, int, int, str]], expected_ids: list[int], query_value: float) -> None:
    assert [row[0] for row in actual] == expected_ids
    for item_id, score, aaa, bbb, ccc, text in actual:
        expected_aaa, expected_bbb, expected_ccc, expected_text = metadata_values(item_id)
        expected_score = l2_distance_sq([query_value] * DIM, native_sequential_vector(item_id, DIM))
        assert abs(score - expected_score) < 1e-4
        assert (aaa, bbb, ccc, text) == (expected_aaa, expected_bbb, expected_ccc, expected_text)


def test_duckdb_staged_write_then_knn(tmp_path, duckdb_con):
    dataset_root = tmp_path / "db"
    dataset_root.mkdir()
    dataset_name = "items"

    with Sketch2Api(dataset_root) as api:
        api.create_dataset(dataset_name, [], dim=4, type_name="f32", range_size=1000, dist_func="dot")
        api.start_writing()
        api.write_vector(10, "0.0, 0.0, 0.0, 0.0")
        api.write_vector(20, "5.0, 5.0, 5.0, 5.0")
        api.write_vector(30, "10.0, 10.0, 10.0, 10.0")
        api.complete_writing()

    assert duckdb_con.execute("SELECT * FROM sketch2_open(?, ?)", [str(dataset_root), dataset_name]).fetchall() == [(True,)]
    rows = duckdb_con.execute(
        "SELECT id, score FROM sketch2_knn(?, ?, NULL) ORDER BY score DESC",
        ["1.0, 1.0, 1.0, 1.0", 2],
    ).fetchall()
    assert [row[0] for row in rows] == [30, 20]


def test_duckdb_staged_delete_updates_knn_visibility(tmp_path, duckdb_con):
    dataset_root = tmp_path / "db"
    dataset_root.mkdir()
    dataset_name = "items"

    with Sketch2Api(dataset_root) as api:
        api.create_dataset(dataset_name, [], dim=4, type_name="f32", range_size=1000, dist_func="dot")
        api.start_writing()
        api.write_vector(10, "0.0, 0.0, 0.0, 0.0")
        api.write_vector(20, "5.0, 5.0, 5.0, 5.0")
        api.complete_writing()
        api.start_writing()
        api.write_deleted(10)
        api.complete_writing()

    duckdb_con.execute("SELECT * FROM sketch2_open(?, ?)", [str(dataset_root), dataset_name])
    rows = duckdb_con.execute(
        "SELECT id FROM sketch2_knn(?, ?, NULL) ORDER BY score DESC",
        ["0.0, 0.0, 0.0, 0.0", 1],
    ).fetchall()
    assert rows == [(20,)]


def test_duckdb_staged_abort_discards_session_and_allows_restart(tmp_path, duckdb_con):
    dataset_root = tmp_path / "db"
    dataset_root.mkdir()
    dataset_name = "items"

    with Sketch2Api(dataset_root) as api:
        api.create_dataset(dataset_name, [], dim=4, type_name="f32", range_size=1000, dist_func="dot")
        api.start_writing()
        api.write_vector(10, "0.0, 0.0, 0.0, 0.0")
        api.abort_writing()
        api.start_writing()
        api.write_vector(20, "5.0, 5.0, 5.0, 5.0")
        api.complete_writing()

    duckdb_con.execute("SELECT * FROM sketch2_open(?, ?)", [str(dataset_root), dataset_name])
    rows = duckdb_con.execute(
        "SELECT id FROM sketch2_knn(?, ?, NULL) ORDER BY score DESC",
        ["5.0, 5.0, 5.0, 5.0", 1],
    ).fetchall()
    assert rows == [(20,)]


def test_duckdb_knn_basic_query(tmp_path, duckdb_con):
    dataset_root = tmp_path / "db"
    dataset_root.mkdir()
    dataset_name = "items"
    create_sequential_dataset(dataset_root, dataset_name)

    duckdb_con.execute("SELECT * FROM sketch2_open(?, ?)", [str(dataset_root), dataset_name])
    query_value = 7.4
    k = 5
    actual = duckdb_con.execute(
        "SELECT id, score FROM sketch2_knn(?, ?, NULL) ORDER BY score, id",
        [query_vector(query_value), k],
    ).fetchall()
    expected = expected_knn_rows(query_value, k)
    assert_knn_rows_equal(actual, expected)


def test_duckdb_knn_empty_dataset_returns_no_rows(tmp_path, duckdb_con):
    dataset_root = tmp_path / "db"
    dataset_root.mkdir()
    dataset_name = "items"

    with Sketch2Api(dataset_root) as api:
        api.create_dataset(dataset_name, [], dim=DIM, type_name="f32", range_size=1000, dist_func="l2")

    duckdb_con.execute("SELECT * FROM sketch2_open(?, ?)", [str(dataset_root), dataset_name])
    rows = duckdb_con.execute(
        "SELECT id, score FROM sketch2_knn(?, ?, NULL)",
        [query_vector(7.4), 5],
    ).fetchall()
    assert rows == []


def test_duckdb_join_knn_results_with_metadata(tmp_path, duckdb_con):
    dataset_root = tmp_path / "db"
    dataset_root.mkdir()
    dataset_name = "items"
    create_sequential_dataset(dataset_root, dataset_name)

    duckdb_con.execute("SELECT * FROM sketch2_open(?, ?)", [str(dataset_root), dataset_name])
    load_metadata_table(duckdb_con, tmp_path)
    query_value = 7.4
    k = 4
    actual = duckdb_con.execute(
        """
        SELECT n.id, n.score, m.aaa, m.bbb, m.ccc, m.text
        FROM sketch2_knn(?, ?, NULL) AS n
        JOIN metadata AS m ON m.id = n.id
        ORDER BY n.score, n.id
        """,
        [query_vector(query_value), k],
    ).fetchall()
    expected_ids = [item_id for item_id, _ in expected_knn_rows(query_value, k)]
    assert_join_rows_match_ids(actual, expected_ids, query_value)


def test_duckdb_join_with_metadata_filters_after_knn(tmp_path, duckdb_con):
    dataset_root = tmp_path / "db"
    dataset_root.mkdir()
    dataset_name = "items"
    create_sequential_dataset(dataset_root, dataset_name)

    duckdb_con.execute("SELECT * FROM sketch2_open(?, ?)", [str(dataset_root), dataset_name])
    load_metadata_table(duckdb_con, tmp_path)
    query_value = 7.4
    k = 6
    actual = duckdb_con.execute(
        """
        SELECT n.id, n.score, m.aaa, m.bbb, m.ccc, m.text
        FROM sketch2_knn(?, ?, NULL) AS n
        JOIN metadata AS m ON m.id = n.id
        WHERE m.aaa = 1
        ORDER BY n.score, n.id
        """,
        [query_vector(query_value), k],
    ).fetchall()
    baseline_ids = [item_id for item_id, _ in expected_knn_rows(query_value, k)]
    expected_ids = [item_id for item_id in baseline_ids if item_id % 2 == 1]
    assert_join_rows_match_ids(actual, expected_ids, query_value)


def test_duckdb_pushdown_allowed_ids_from_metadata_subquery(tmp_path, duckdb_con):
    dataset_root = tmp_path / "db"
    dataset_root.mkdir()
    dataset_name = "items"
    create_sequential_dataset(dataset_root, dataset_name)

    duckdb_con.execute("SELECT * FROM sketch2_open(?, ?)", [str(dataset_root), dataset_name])
    load_metadata_table(duckdb_con, tmp_path)
    query_value = 7.4
    k = 6
    filter_ref = bitset_filter_ref_for_predicate(duckdb_con, "aaa = 1")
    actual = duckdb_con.execute(
        """
        SELECT n.id, n.score
        FROM sketch2_knn(?, ?, ?) AS n
        ORDER BY n.score, n.id
        """,
        [query_vector(query_value), k, filter_ref],
    ).fetchall()
    allowed_ids = {item_id for item_id in range(START_ID, START_ID + COUNT) if item_id % 2 == 1}
    expected = expected_knn_rows(query_value, k, allowed_ids=allowed_ids)
    assert_knn_rows_equal(actual, expected)


def test_duckdb_pushdown_with_different_metadata_predicates(tmp_path, duckdb_con):
    dataset_root = tmp_path / "db"
    dataset_root.mkdir()
    dataset_name = "items"
    create_sequential_dataset(dataset_root, dataset_name)

    duckdb_con.execute("SELECT * FROM sketch2_open(?, ?)", [str(dataset_root), dataset_name])
    load_metadata_table(duckdb_con, tmp_path)
    query_value = 12.35
    k = 4
    cases = [
        ("bbb_in_1_3", "bbb IN (1, 3)", {item_id for item_id in range(START_ID, START_ID + COUNT) if item_id % 5 in (1, 3)}),
        ("ccc_between_2_6", "ccc BETWEEN 2 AND 6", {item_id for item_id in range(START_ID, START_ID + COUNT) if 2 <= (item_id % 10) <= 6}),
    ]
    for _, predicate_sql, allowed_ids in cases:
        filter_ref = bitset_filter_ref_for_predicate(duckdb_con, predicate_sql)
        actual = duckdb_con.execute(
            """
            SELECT n.id, n.score
            FROM sketch2_knn(?, ?, ?) AS n
            ORDER BY n.score, n.id
            """,
            [query_vector(query_value), k, filter_ref],
        ).fetchall()
        expected = expected_knn_rows(query_value, k, allowed_ids=allowed_ids)
        assert_knn_rows_equal(actual, expected)


def test_duckdb_pushdown_and_join_return_metadata_columns(tmp_path, duckdb_con):
    dataset_root = tmp_path / "db"
    dataset_root.mkdir()
    dataset_name = "items"
    create_sequential_dataset(dataset_root, dataset_name)

    duckdb_con.execute("SELECT * FROM sketch2_open(?, ?)", [str(dataset_root), dataset_name])
    load_metadata_table(duckdb_con, tmp_path)
    query_value = 12.35
    k = 4
    allowed_ids = {item_id for item_id in range(START_ID, START_ID + COUNT) if item_id % 5 in (1, 3)}
    filter_ref = bitset_filter_ref_for_predicate(duckdb_con, "bbb IN (1, 3)")
    actual = duckdb_con.execute(
        """
        SELECT n.id, n.score, m.aaa, m.bbb, m.ccc, m.text
        FROM sketch2_knn(?, ?, ?) AS n
        JOIN metadata AS m ON m.id = n.id
        ORDER BY n.score, n.id
        """,
        [query_vector(query_value), k, filter_ref],
    ).fetchall()
    expected_ids = [item_id for item_id, _ in expected_knn_rows(query_value, k, allowed_ids=allowed_ids)]
    assert_join_rows_match_ids(actual, expected_ids, query_value)


def test_duckdb_pushdown_changes_neighbor_set_vs_postfilter(tmp_path, duckdb_con):
    dataset_root = tmp_path / "db"
    dataset_root.mkdir()
    dataset_name = "items"
    create_sequential_dataset(dataset_root, dataset_name)

    duckdb_con.execute("SELECT * FROM sketch2_open(?, ?)", [str(dataset_root), dataset_name])
    load_metadata_table(duckdb_con, tmp_path)
    query_value = 7.4
    k = 6

    post_filtered = duckdb_con.execute(
        """
        SELECT n.id
        FROM sketch2_knn(?, ?, NULL) AS n
        JOIN metadata AS m ON m.id = n.id
        WHERE m.aaa = 1
        ORDER BY n.score, n.id
        """,
        [query_vector(query_value), k],
    ).fetchall()

    filter_ref = bitset_filter_ref_for_predicate(duckdb_con, "aaa = 1")
    pushed_down = duckdb_con.execute(
        """
        SELECT n.id
        FROM sketch2_knn(?, ?, ?) AS n
        ORDER BY n.score, n.id
        """,
        [query_vector(query_value), k, filter_ref],
    ).fetchall()

    assert [row[0] for row in post_filtered] == [7, 9, 5]
    assert [row[0] for row in pushed_down] == [7, 9, 5, 11, 3, 13]
