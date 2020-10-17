#!/usr/bin/env python3
"""
Schema creator class for datapy.
"""
import os
import sys

from datapy import variables

class Schema:
    """ Class which defines schema methods and objects
    Args:
        schema_root (str): the root directory of schemas. Default is cwd.
                           If DATAPY_SCHEMA_ROOT is set, will use this.
        schema_name (str): the name of the schema, default is default
    """

    def __init__(self, schema_name="default"):
        self.schema_name = schema_name

    @property
    def schema_root(self):
        """ Sets the schema root and adds it to the class
        """
        schema_root = variables.OS_SCHEMA_ROOT
        if schema_root in os.environ:
            return os.environ[schema_root]
        else:
            return os.getcwd()

    def create_schema(self):
        """ Creates a schema in schema root.
        """
        path = os.path.join(self.schema_root, self.schema_name)
        os.makedirs(path)
