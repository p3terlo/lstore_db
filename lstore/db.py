import os

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
        self.bufferpool.assign_path(path)

        # Create path for files to be stored
        try:
            os.mkdir(path)
        except FileExistsError:
            print("Path Already Made")


    def close(self):
        self.bufferpool.evict_all()
        for i in self.tables:
            self.tables[i].endBackground()
        # self.bufferpool.print_pool()
        # pass


    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        table.pass_bufferpool(self.bufferpool)
        table.setUpBackgroundThread()
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


    # def get_table(self, name):
    #     try:
    #         table = self.tables[name]
    #         return table
    #     except KeyError:
    #         print("get_table Error: No table exists with name %s" % (name))


    def get_table(self, table_name: str):
        """
        Read persisted records to create a table. 
        Involves populating primary index as well as 
        populating the page directory. 
        """
        if table_name in self.tables:
            return self.tables[table_name]
        
        num_hidden_columns = primary_key_column_id = 4

        number_columns = rid_column_id = 0

        number_of_slots = int(PAGE_CAPACITY_IN_BYTES / INTEGER_CAPACITY_IN_BYTES)
        base_record_filenames = [file_name for file_name in os.listdir(self.path) if "tail" not in file_name]

        for file_name in base_record_filenames:
            if table_name in file_name:
                number_columns += 1
        
        table = Table(table_name, number_columns - num_hidden_columns, 0)
        table.pass_bufferpool(self.bufferpool)
        bp = table.bufferpool

        for primary_key_page, rid_page in zip(
            bp.scan_column_pages(column_id=primary_key_column_id, number_columns=number_columns, table_name="Grades"),
            bp.scan_column_pages(column_id=rid_column_id, number_columns=number_columns, table_name="Grades")):
            
            for slot_number in range(number_of_slots):
                key = primary_key_page.grab_slot(slot_number)
                rid = rid_page.grab_slot(slot_number)
                pd_vals = table.calculate_base_page_numbers(number_columns, rid) 
                
                table.index.insert(key, rid)
                table.page_directory[rid] = pd_vals    
        
        self.tables[table_name] = table
        return table
