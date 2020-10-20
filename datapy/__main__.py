#!/usr/bin/env python3
"""
Main entrypoint for datapy
Currently a POC without an API
"""

from datapy.Schema import Schema
from datapy.Table import Table

if __name__ == "__main__":
    # This current main implementation is for test usage and not the actual program
    # Create a Schema
    Schema('Julia').create_schema()
    # Create a Table instance
    # ../data/demo.csv is an example of a csv file to be used
    # can view this with cat ../data/demo.csv from this directory
    table = Table('Julia', '../data/demo.csv')
    # Start a db session and ingest csv data into sqlite3 db then dump to a file
    with table.db_session(table.table_root) as c:
        table.ingest_csv(c)
        print(table.select_all_records(c))
        table.dump_table_to_file(c)

    # load the contents of the db file dumped by the above into an instance of table2
    table2 = Table('Julia', None, 'demo.db')
    # select all the records
    with table2.db_session(table2.table_root) as c:
        print(table2.select_all_records(c))
        
