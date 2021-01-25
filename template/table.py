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

    def display(self):
        print(self.rid)
        print(self.key)
        print(self.columns)
        
class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.pages = []
        pass

    def __merge(self):
        pass

    def display(self):
        print()
        print(self.name)
        print(self.key)
        print(self.num_columns)
        print(self.page_directory)
       # print(self.container)
       # print("int")
        #self.index.display()
        #self.pages[0].display()
        print()
        for i in range(4):
            self.pages[i].display()
            

    def add(self, *columns):
        if len(self.pages) == 0:
            for i in range(4):
                new_page = Page()
                self.pages.append(new_page)
                #print(i)

        #GRABBING TUPLE
        vals = columns

       #rid logic = location of all pages (needs to be updated)
        rid = self.pages[0].num_records

        local_time = time()

        #definied by macros
        col = (0,rid,local_time, 0)
        key = vals[0][0] #id
        
        new_record = Record(rid, key, col)
        #new_record.display()
        
        
        for i in range(4):
            self.pages[i].write(vals[0][i + 1])
            #print(vals[0][i + 1])
        self.page_directory[key] = rid
 
