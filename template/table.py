from template.page import *
from template.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, bpool):
        self.name = name
        self.key = key
        self.num_columns = num_columns


        
        self.page_directory = {}
        self.index = Index(self)



        #------main---------
        self.pool = bpool
        self.pool.init_poolkey(name)
        #self.pool.display()
        
        self.base_rid = 1
        pass

    def __merge(self):
        pass

    #return rid and increments it
    def get_rid(self):
        old_rid = self.base_rid
        self.base_rid += 1
        return old_rid
        
    
    def add(self, *columns):
        #reducing clutter
        table_name = self.name
        rid = self.get_rid()
        
        test_data = []
        test_data.append(rid)

        for col in columns:
            test_data.append(col)
        
        
        self.pool.add(table_name, test_data)
        
        
        
        
        #print(*columns)
        pass

    def display_pool(self):
        print("current pool")
        self.pool.display(self.name)
