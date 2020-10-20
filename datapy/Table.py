#!/usr/bin/env python3
"""
Table class for datapy.
"""

import os
import sys
import csv
import sqlite3 as sql
from _io import TextIOWrapper

from datapy import variables # don't know if I'll need this yet
from datapy.Schema import Schema

class Table(Schema):
    """ Table creation, deletion, and manipulation
    Args:
        schema_name (Schema object): the name of the schema to place the file
        input_data (str): the path to an input file
        table_db (str): name of table to access
    """
    def __init__(self, schema_name, input_data=None, table_db=None):
        super().__init__(schema_name)
        self.input_data = input_data
        self.table_db = table_db
        if table_db == None and input_data == None:
            sys.exit("Need one of input_data or table_name")

    @property
    def table_name(self):
        """ Determine the table_name, if a db file already exists, we use that.
            if not, we use the name of the csv file
        Args:
            self.table_db (File): previously dumped sqlite database file
            self.input_data (File): new input data in csv format
        Returns:
            self.table_name (str) : the name of the table
        """
        if self.table_db:
            return os.path.basename(os.path.splitext(self.table_db)[0])
        else:
            return os.path.basename(os.path.splitext(self.input_data)[0])
        
    @property
    def table_root(self):
        """ We need all functions to be able to access the table, so we add it as a property """
        # if using a csv file for a table that didn't already exist then
        # we should create a .db file
        if self.table_db == None:
            outfile = str(self.table_name) + '.db'
            table_root = os.path.join(self.schema_path, outfile)
            return table_root
        else:
            table = os.path.basename(table_db)
            return os.path.join(self.schema_path, table)
    
    @staticmethod
    def db_session(table_root):
        """ Establish a connection with a sqlite database

        If a file path is given, returns a connection to that database file,
        otherwise returns a connection to an in-memory database.

        Args:
            self.table_root (Optional[str], optional): If provided, a file path to a
            persistent database file. Defaults to None.

        Returns:
            sql.Connection: connection to the database
        """
        # if db file exists already
        if os.path.isfile(table_root):
            conn = sql.connect(":memory:")
            sql_file = open(table_root)
            sql_string = sql_file.read()
            conn.executescript(sql_string)
            return conn
        return sql.connect(":memory:")
    
    def create_table(self, conn, headers):
        """ Create a table given the headers
        Args:
            self.table_name (str): name of the table to create
            conn (sql.Connection): connection to the database
            table_header (str): table headers in string format -
                                "field::type,field::type"

        Returns:
            bool: Was the table created?
        """
        headers = [h.replace("::", " ") for h in headers.split(",")]
        create_stmt = f"create table if not exists {self.table_name} ( {', '.join(headers)} )"
        c = conn.cursor()
        try:
            c.execute(create_stmt)
            return True
        except:
            return False

    def load_table(self, conn):
        """ loads tables from a previous db dump
        Args:
            self.table_root (str): path to table
            conn (sql.Connection): connection to the database
        """
        c = conn.cursor()
        
    def ingest_csv(self, conn):
        """ Read a properly formatted .csv file and stash it in a sqlite table
        Args:
            self.input_data (str): path to the .csv file
            conn (sql.Connection): connection to the database
        Returns:
            int: the number of table rows inserted
        """
        with open(self.input_data) as f, conn:
            headers = f.readline().strip()
            created = self.create_table(conn, headers)
            if created:
                rows_inserted = self.insert_rows(conn, headers, f)
            try:
                conn.commit()
                return rows_inserted
            except:
                sys.exit("No rows were inserted, error")

    def insert_rows(self, conn, headers, file_conn):
        """ Insert rows from a csv file connection into a database table
        Args:
            self.table_name (str): name of the table to insert records into
            conn (sql.Connection): connection to the database
            headers (str): table headers in string format -
                           "field::type,field::type"
            file_conn (TextIOWrapper): file connection
                  this is the text stream opened in with statement on line 112
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
                f"insert into {self.table_name} ({','.join(headers)}) "
                f"values ({','.join(['?']*len(headers))});"
                )
            # should we check the length of headers against the length of vals?
            c.execute(insert_stmt, tuple(row.values()))
            rows_inserted += 1
        return rows_inserted

    def select_all_records(self, conn):
        """ Return all records from a named table
        Args:
            self.table_name (str): name of the table to fetch from
            conn (sql.Connection): connection to the database

        Returns:
            List[Tuple[Any]]: A list of returned records
        """
        with conn:
            c = conn.cursor()
            print(self.table_name)
            try:
                c.execute(f"select * from {self.table_name}")
                # I would like this to call another function that allows for args
                # so that we aren't implementing hardcoded selects in multiple places
                return c.fetchall()
            except Exception as e:
                print(e)
                sys.exit(f"Couldn't select records from {self.table_name}")

    def dump_table_to_file(self, conn):
        """ Dumps all records in current table to file in schema
        Args:
            self.table_root (File): path to output file
            conn (sql.Connection): connection to the database
        Returns: None
        """
        # perhaps this should mention that it is for dumping in memory tables to a file
        # additionally this might also want to create the file for the table that could
        # be used later, need Eric's feedback on that
        with open(self.table_root, 'w') as f:
            for line in conn.iterdump():
                f.write(f'{line}\n')
            
