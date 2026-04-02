from __future__ import annotations

import sqlite3 as sql

from database_connector import DatabaseService, SQLiteDialect


def test_insert_ignore_template_ignores_duplicates() -> None:
    con = sql.connect(":memory:")
    db = DatabaseService(con, SQLiteDialect())

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
    insert_sql = db.build_insert_ignore(table="t", columns=("id", "val"))
    db.executemany(insert_sql, [(1, "a"), (1, "a")])
    db.commit()

    rows = db.fetchall("SELECT id, val FROM t")
    assert rows == [(1, "a")]


def test_upsert_template_updates_existing_row() -> None:
    con = sql.connect(":memory:")
    db = DatabaseService(con, SQLiteDialect())

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, note TEXT)")
    upsert_sql = db.build_upsert(
        table="t",
        columns=("id", "val", "note"),
        conflict_columns=("id",),
        update_columns=("val", "note"),
        coalesce_update_columns=("note",),
    )

    db.execute(upsert_sql, (1, "v1", "n1"))
    db.execute(upsert_sql, (1, "v2", None))
    db.commit()

    row = db.fetchone("SELECT id, val, note FROM t WHERE id = 1")
    assert row == (1, "v2", "n1")
