"""
Proof-of-concept for reading a *.csv into an in-memory database and
querying data from it.
"""

import csv
import os
import sqlite3 as sql
from _io import TextIOWrapper
from typing import Any, List, Optional, Tuple


def db_session(path: Optional[str] = None) -> sql.Connection:
    """Establish a connection with a sqlite database

    If a file path is given, returns a connection to that database file,
    otherwise returns a connection to an in-memory database.

    Args:
        path (Optional[str], optional): If provided, a file path to a
        persistent database file. Defaults to None.

    Returns:
        sql.Connection: connection to the database
    """
    if path:
        return sql.connect(path)
    return sql.connect(":memory:")


def ingest_csv(conn: sql.Connection, path: str) -> int:
    """Read a properly formatted .csv file and stash it in a sqlite
    table

    Args:
        path (str): path to the .csv file

    Returns:
        int: the number of table rows inserted
    """
    filename = os.path.basename(path).split(".")[0]
    with open(path) as f, conn:
        headers = f.readline().strip()
        created = create_table(conn, filename, headers)
        if created:
            rows_inserted = insert_rows(conn, filename, headers, f)
        try:
            conn.commit()
            return rows_inserted
        except:  # TODO: Add actual error handling
            return 0

    return 0


def create_table(conn: sql.Connection, name: str, headers: str) -> bool:
    """Create a table given the headers

    Args:
        conn (sql.Connection): connection to the database
        name (str): name of the table to create
        headers (str): table headers in string format -
        "field::type,field::type"

    Returns:
        bool: Was the table created?
    """
    headers = [h.replace("::", " ") for h in headers.split(",")]
    create_stmt = f"create table if not exists {name} ( {', '.join(headers)} )"
    c = conn.cursor()
    try:
        c.execute(create_stmt)
        return True
    except:  # TODO: Add actual error handling
        return False


def insert_rows(
    conn: sql.Connection, tablename: str, headers: str, file_conn: TextIOWrapper
) -> int:
    """Insert rows from a csv file connection into a database table

    Args:
        conn (sql.Connection): connection to the database
        tablename (str): name of the table to insert records into
        headers (str): table headers in string format -
        "field::type,field::type"
        file_conn (TextIOWrapper): file connection

    Returns:
        int: number of records inserted
    """
    types = [h.split("::")[1] for h in headers.split(",")]
    headers = [h.split("::")[0] for h in headers.split(",")]
    reader = csv.DictReader(file_conn, fieldnames=headers)
    rows_inserted = 0
    c = conn.cursor()

    for row in reader:
        insert_stmt = (
            f"insert into {tablename} ({','.join(headers)}) "
            f"values ({','.join(['?']*len(headers))});"
        )
        c.execute(insert_stmt, tuple(row.values()))
        rows_inserted += 1
    return rows_inserted


def select_all_records(conn: sql.Connection, tablename: str) -> List[Tuple[Any]]:
    """Return all records from a named table

    Args:
        conn (sql.Connection): connection to the database
        tablename (str): name of the table to fetch from

    Returns:
        List[Tuple[Any]]: A list of returned records
    """
    with conn:
        c = conn.cursor()
        try:
            c.execute(f"select * from {tablename}")  # TODO: This is dangerous
            return c.fetchall()
        except Exception as e:  # TODO: Add actual error handling
            print(e)
            return None


def main():
    # Read the csv file into a table
    with db_session() as conn:
        ingest_csv(conn, "../data/demo.csv")
        records = select_all_records(conn, "demo")
        print(records)


if __name__ == "__main__":
    main()
