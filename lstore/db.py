from lstore.config import *
from lstore.table import Table
from lstore.bufferpool import BufferPool


class Database():

    def __init__(self):
        self.path = ""
        self.tables = {}
        self.bufferpool = BufferPool(BUFF_POOL_SIZE)


    def open(self, path):
        self.path = path

        # Create path for files to be stored
        try:
            os.mkdir(path)
        except FileExistsError:
            print("Path Already Made")


    def close(self):
        pass


    def create_table(self, name, num_columns, key, buffer_pool):
        table = Table(name, num_columns, key, self.bufferpool)
        if name in self.tables:
            print("create_table Error: Table with name %s already exists" % (name))
        else:
            self.tables[name] = table
            return table


    def drop_table(self, name):
        try:
            self.tables.pop(name)
        except KeyError:
            print("drop_table Error: No table exists with name %s" % (name))


    def get_table(self, name):
        try:
            table = self.tables[name]
            return table
        except KeyError:
            print("get_table Error: No table exists with name %s" % (name))