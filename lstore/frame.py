import os

from lstore.config import *


class Frame:
    """
    Container for holding pages in bufferpool.
    """

    def __init__(self, page_num, page, table, num_col):
        self.key = page_num
        self.page = page
        self.table_name = table
        self.outstanding_transactions = 0
        self.num_columns = num_col
        self.is_dirty = False
        self.is_tail = False


    def pin_page(self):
        self.outstanding_transactions += 1


    def unpin_page(self):
        if self.outstanding_transactions > 0:
            self.outstanding_transactions -= 1


    def make_dirty(self):
        self.is_dirty = True

    
    def clean_page(self):
        self.is_dirty = False


    def write_slot(self, rid, data):#Alvin
        self.pin_page()
        self.page.write_slot(rid, data)
        self.make_dirty()
        self.unpin_page()


    def write_value(self, val):
        self.page.write(val)
        self.is_dirty = True


    def read_frame(self, path):
        page_num = self.key
        num_col = self.num_columns #
        
        seek_offset = int(page_num/num_col)
        seek_mult = PAGE_CAPACITY_IN_BYTES

        file_num = page_num % num_cols
        file_name = self.path + "/" + table_name + "_" + str(file_num) + ".bin"

        file = open(file_name, "rb")
        file.seek(seek_offset * seek_mult)
        data = file.read(seek_mult)
        file.close()


        self.page.data = data
        


    def write_frame(self, path):

        page_num = self.key
        num_col = self.num_columns
        seek_offset = int(page_num/num_col)
        seek_mult = PAGE_CAPACITY_IN_BYTES
        file_num = page_num % num_col
        page  = self.page
        file_name = path + "/" + self.table_name + "_" + str(file_num) + ".bin"
        # print("File num:",file_num)

        mode = "w+b"
        if os.path.exists(file_name):
            mode = "r+b"

        file= open(file_name, mode) #binary
        file.seek(seek_offset * seek_mult)
        file.write(page.data)
        file.close()
        
        self.is_dirty = False


    def write_frame_tail(self, path):

        page_num = self.key
        num_col = self.num_columns
        seek_offset = int(page_num/num_col)
        seek_mult = PAGE_CAPACITY_IN_BYTES
        file_num = page_num % num_col
        page  = self.page
        file_name = path + "/" + self.table_name + "_tail_" + str(file_num) + ".bin"
        # print("File num:",file_num)

        mode = "w+b"
        if os.path.exists(file_name):
            mode = "r+b"

        file= open(file_name, mode) #binary
        file.seek(seek_offset * seek_mult)
        file.write(page.data)
        file.close()
        
        self.is_dirty = False

    def print_page(self):
        print(f"Page Identity: {self.key, self.page, self.table_name}")
        self.page.display_internal_memory()
