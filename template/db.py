from template.table import Table
from template.bufferpool import *

import os

class Database():

    def __init__(self):
        self.tables = []
        self.bufferpool = Bufferpool()
        pass

    # Not required for milestone1
    def open(self, path):

        self.bufferpool.assign_path(path)

        #creating path for files to be stored
        try:
            os.mkdir(path)
        except FileExistsError:
            print("Path Already Made")

        
        
        pass

    def close(self):
        if (self.bufferpool.cur_cap() != 0):
            print("curcap: " , self.bufferpool.cur_cap())
            self.bufferpool.db_close()
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        #file = self.path + '/' + name
        #test = open(file, "w")
        
        table = Table(name, num_columns, key, self.bufferpool)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        pass
