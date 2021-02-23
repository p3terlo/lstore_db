from lstore.config import *
from lstore.table import Table
from lstore.bufferpool import *

class Database():

    def __init__(self):
        self.path = ""
        self.bufferpool = BufferPool()


    def open(self, path):
        self.path = path

        # Create path for files to be stored
        try:
            os.mkdir(path)
        except FileExistsError:
            print("Path Already Made")


    def close(self):
        pass


    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        if name in self.bufferpool:
            print("create_table Error: Table with name %s already exists" % (name))
        else:
            self.bufferpool.pool[name] = BufferTable(name, table)
            return table


    def drop_table(self, name):
        try:
            self.bufferpool.pop(name)
        except KeyError:
            print("drop_table Error: No table exists with name %s" % (name))


    def get_table(self, name):
        try:
            table = self.bufferpool[name].table
            return table
        except KeyError:
            print("get_table Error: No table exists with name %s" % (name))