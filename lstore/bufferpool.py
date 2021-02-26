import os

from collections import OrderedDict
from lstore.config import *
from lstore.frame import Frame
from lstore.page import Page


class BufferPool: 

    def __init__(self, capacity: int):
        self.path = ""
        self.frame_cache = OrderedDict()
        self.capacity = capacity
        self.total_db_pages = 0
        self.number_current_pages = 0
        
        
    def assign_path(self, path):
        self.path = path


    def get_page(self, page_num: int) -> None:
        #TODO: Increment Pin count 
        if page_num not in self.frame_cache:
            raise KeyError(f"Invalid Page: {page_num}. Not in Queue.")
        else:
            self.frame_cache.move_to_end(page_num)
            return self.frame_cache[page_num]
 

    def add_page(self, page, table_name: str, num_col: int) -> None:
        # print(f"Attempting to add page: {page.page_num} to Buffer Queue")
        
        if (self.number_current_pages >= self.capacity):
            self.evict_recently_used()

        frame = Frame(page.page_num, page, table_name, num_col)
        
        self.number_current_pages += 1
        self.frame_cache[page.page_num] = frame
        self.frame_cache.move_to_end(page.page_num)

        print(f"Placed page: {page.page_num} inside frame")


    def print_pool(self):
        if len(self.frame_cache) == 0:
            print("BufferPool is empty")
        else:
            for frame in self.frame_cache.values():
                frame.print_page()


    def evict_recently_used(self, use_most_recently_used = False):    

        # Check the outstanding transactions of the least recently used frame
        # evicted_frame_pin_count = next(
        #     reversed(self.frame_cache.values())).outstanding_transactions
                
        # if evicted_frame_pin_count == 0:
                
        _, lru_frame = self.frame_cache.popitem(last = use_most_recently_used)
        self.number_current_pages -= 1
        print(f"Evicting LRU frame: {lru_frame.page.page_num}")
    
        key = lru_frame.key
        is_dirty = lru_frame.is_dirty

        if (is_dirty):
            print("Persisting LRU Frame ", key)
            lru_frame.write_frame(self.path)
                

    def pin_page(self, page_num):
        frame = self.get_page(page_num)
        frame.pin_page()


    def unpin_page(self, page_num):
        frame = self.get_page(page_num)
        frame.unpin_page()


    def check_pool(self, page_id):
        # print(f"Checking if Page {page_id} exists inside the bufferpool: ", page_id in self.frame_cache)
        return page_id in self.frame_cache


    def read_page_from_disk(self, table_name, page_num, num_cols):
        print(f"Reading {table_name}: {page_num} from disk...")
        seek_offset = int(page_num/num_cols)
        seek_mult = PAGE_CAPACITY_IN_BYTES

        file_num = page_num % num_cols
        file_name = self.path + "/" + table_name + "_" + str(file_num) + ".bin"

        if not os.path.exists(file_name):
            assert FileNotFoundError(f"FAILED reading {table_name}: {page_num} from disk.\n")
        else:
            page = Page(page_num)
            with open(file_name, "rb") as f:
                f.seek(seek_offset * seek_mult)
                data = f.read(seek_mult)
                page.data = bytearray(data)
            return page