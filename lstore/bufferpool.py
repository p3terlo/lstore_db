import os
import sys

from collections import OrderedDict
from lstore.config import *
from lstore.frame import Frame
from lstore.page import Page


class BufferPool: 

    def __init__(self, capacity: int):
        self.path = ""
        self.frame_cache = OrderedDict()
        self.capacity = capacity
        self.page_identifier = 0
        self.number_current_pages = 0
        
        
    def assign_path(self, path):
        self.path = path


    def make_new_frame(self, table_name, num_columns, page_num): #Alvin
        #print("making new frame", page_num)        
        new_page = Page(page_num)     
        frame = Frame(page_num, new_page, table_name, num_columns)

        #always add to pool
        self.frame_cache[page_num] = frame
        self.frame_cache.move_to_end(page_num)
        self.number_current_pages += 1

        return frame


    def read_frame(self, table_name, num_columns, page_num): #Alvin
        file_num = page_num % num_columns
        file_name = self.path + "/" + table_name + "_" + str(file_num) + ".bin"
       # print("making new page/frame")

        if not os.path.exists(file_name): 
            return self.make_new_frame(table_name,num_columns,page_num)
        
        #reading from FILE
        seek_offset = int(page_num/num_columns)
        seek_mult = PAGE_CAPACITY_IN_BYTES
        
        page = Page(page_num)
        with open(file_name, "rb") as f:
            f.seek(seek_offset * seek_mult)
            data = f.read(seek_mult)
            page.data = bytearray(data)
            if sys.getsizeof(data) < 80:
                print("Allocating space for empty page of size:",sys.getsizeof(data))
                page.data = bytearray(PAGE_CAPACITY_IN_BYTES)

        frame = Frame(page_num, page, table_name, num_columns)
        self.frame_cache[page_num] = frame
        self.frame_cache.move_to_end(page_num)
        self.number_current_pages += 1
        return frame


    
    def read_frame_tail(self, table_name, num_columns, page_num): #Alvin
        file_num = page_num % num_columns
        file_name = self.path + "/" + table_name + "_tail_" + str(file_num) + ".bin"
       # print("making new page/frame")

        if not os.path.exists(file_name): 
            return self.make_new_frame(table_name,num_columns,page_num)

        #reading from FILE
        seek_offset = int(page_num/num_columns)
        seek_mult = PAGE_CAPACITY_IN_BYTES
        
        page = Page(page_num)
        with open(file_name, "rb") as f:
            f.seek(seek_offset * seek_mult)
            data = f.read(seek_mult)
            page.data = bytearray(data)
            if sys.getsizeof(data) < 80:
                print("Allocating space for empty page of size:",sys.getsizeof(data))
                page.data = bytearray(PAGE_CAPACITY_IN_BYTES)

        frame = Frame(page_num, page, table_name, num_columns)

        page_to_put_in_pool = (page_num * -1) - num_columns
        # page_to_put_in_pool = page_num

        self.frame_cache[page_to_put_in_pool] = frame
        self.frame_cache.move_to_end(page_to_put_in_pool)
        self.number_current_pages += 1
        return frame

    def evict(self): #Alvin
        lru_frame = self.frame_cache.popitem(last = False)[-1]
        self.number_current_pages -= 1
        
        key = lru_frame.key
        is_dirty = lru_frame.is_dirty

        if (is_dirty):
            if lru_frame.is_tail == True:
                #print("Persisting tail LRU Frame ", key)
                lru_frame.write_frame_tail(self.path)
                lru_frame.is_tail = False
            else:
                #print("Persisting LRU Frame ", key)
                lru_frame.write_frame(self.path)

    def fetch_frame(self, table_name, number_columns, page_num): #Alvin
        # self.print_pool()

        if page_num not in self.frame_cache:
            if (self.number_current_pages >= self.capacity): #eviction
                self.evict()
            return self.read_frame(table_name, number_columns, page_num)
        else:
            self.frame_cache.move_to_end(page_num)
            return self.frame_cache[page_num]


    def fetch_frame_tail(self, table_name, number_columns, page_num): #Alvin
        page_num = (page_num*-1) - number_columns #0->-9, 1->-10
       # print(table_name, number_columns, page_num)
        if page_num not in self.frame_cache:
            if (self.number_current_pages >= self.capacity): #eviction
                self.evict()
            page_num_to_read = (page_num + number_columns) * -1
            return self.read_frame_tail(table_name, number_columns, page_num_to_read)
        else:
            self.frame_cache.move_to_end(page_num)
            return self.frame_cache[page_num]


    def create_new_page(self, table_name: str, num_columns: int) -> Frame:        
        empty_page = Page(page_num=self.page_identifier)
        self.page_identifier += 1
        frame = Frame(empty_page.page_num, empty_page, table_name, num_columns)

        return frame


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
                
        lru_frame = self.frame_cache.popitem(last = use_most_recently_used)
        self.number_current_pages -= 1
        # print(f"Evicting LRU frame: {lru_frame.page.page_num}")
    
        key = lru_frame.key
        is_dirty = lru_frame.is_dirty

        if (is_dirty):
            if lru_frame.is_tail == True:
            # if lru_frame.page.page_num < 0:
                print("Persisting tail LRU Frame ", key)
                lru_frame.write_frame_tail(self.path)
                lru_frame.is_tail = False
            else:
                print("Persisting LRU Frame ", key)
                lru_frame.write_frame(self.path)
            # print("Persisting LRU Frame ", key)
            # lru_frame.write_frame(self.path)
                

    def check_pool(self, page_id):
        # print(f"Checking if Page {page_id} exists inside the bufferpool: ", page_id in self.frame_cache)
        return page_id in self.frame_cache












 
    # def read_page_from_disk(self, table_name: str, num_columns: int, page_num: int) -> Frame:
    #     """
    #     Given Table Name, its number of columns and it's page number, we grab page from disk.
    #     """

    #     print(f"Reading {table_name}: {page_num} from disk...")
    #     seek_offset = int(page_num/num_columns)
    #     seek_mult = PAGE_CAPACITY_IN_BYTES

    #     file_num = page_num % num_columns
    #     print(f"File Number: {file_num}")
    #     file_name = self.path + "/" + table_name + "_" + str(file_num) + ".bin"

    #     if not os.path.exists(file_name):
    #         print(f"FAILED reading {table_name}: {page_num} from disk.\n")
    #         return None
    #     else:
    #         page = Page(page_num)
    #         with open(file_name, "rb") as f:
    #             f.seek(seek_offset * seek_mult)
    #             data = f.read(seek_mult)
    #             page.data = bytearray(data)
    #             if sys.getsizeof(data) < 80:
    #                 print("Allocating space for empty page of size:",sys.getsizeof(data))
    #                 page.data = bytearray(PAGE_CAPACITY_IN_BYTES)

    #         frame = Frame(page_num, page, table_name, num_columns)            
    #         return frame
 
    # def read_page_from_disk_tail(self, table_name: str, num_columns: int, page_num: int) -> Frame:
    #     """
    #     Given Table Name, its number of columns and it's page number, we grab page from disk.
    #     """

    #     print(f"Reading tail {table_name}: {page_num} from disk...")
    #     seek_offset = int(page_num/num_columns)
    #     seek_mult = PAGE_CAPACITY_IN_BYTES

    #     file_num = page_num % num_columns
    #     print(f"File Number: {file_num}")
    #     file_name = self.path + "/" + table_name + "_tail_" + str(file_num) + ".bin"

    #     if not os.path.exists(file_name):
    #         print(f"FAILED reading tail {table_name}: {page_num} from disk.\n")
    #         return None
    #     else:
    #         page = Page(page_num)
    #         with open(file_name, "rb") as f:
    #             f.seek(seek_offset * seek_mult)
    #             data = f.read(seek_mult)
    #             page.data = bytearray(data)
    #             if sys.getsizeof(data) < 80:
    #                 print("Allocating space for empty page of size:",sys.getsizeof(data))
    #                 page.data = bytearray(PAGE_CAPACITY_IN_BYTES)


    #         frame = Frame(page_num, page, table_name, num_columns)            
    #         return frame

    
 # def get_frame_from_pool(self, table_name: str, number_columns: int, page_num: int):
    #     """
    #     Look in Bufferpool for page, if not found then search disk, if not found create page.
    #     """

    #     if page_num not in self.frame_cache:
    #         print("number_current_pages:", self.number_current_pages, "capactiy:", self.capacity)
    #         if (self.number_current_pages >= self.capacity):
    #             self.evict_recently_used()
    #             # self.number_current_pages += 1
    #         print("Entered: Page not in pool ...")

    #         self.number_current_pages += 1

    #         frame = self.read_page_from_disk(table_name, number_columns, page_num)
    #         print("frame is ",frame.key)
    #         if frame is None:
    #             print("Entered: create_new_page ...")
    #             frame = self.create_new_page(table_name, number_columns)            

    #         self.frame_cache[frame.page.page_num] = frame
    #         self.frame_cache.move_to_end(frame.page.page_num)
            
    #         print(f"Placed page: {frame.page.page_num} inside frame")
    #         print("number_current_pages:", self.number_current_pages, "capactiy:", self.capacity)

    #         return self.frame_cache[frame.page.page_num]


    #         # raise KeyError(f"Invalid Page: {page_num}. Not in Queue.")
    #     else:
    #         print("Entered: frame IN pool ...")
    #         print("number_current_pages:", self.number_current_pages, "capactiy:", self.capacity)

    #         self.frame_cache.move_to_end(page_num)
    #         return self.frame_cache[page_num]


     # def get_frame_from_pool_tail(self, table_name: str, number_columns: int, page_num: int):
    #     """
    #     Look in Bufferpool for page, if not found then search disk, if not found create page.
    #     """
    #     print("PAGENUM:",page_num)
    #     page_num = (page_num*-1) - number_columns #0->-9, 1->-10

    #     if page_num not in self.frame_cache:
    #         print("number_current_pages:", self.number_current_pages, "capactiy:", self.capacity)
    #         if (self.number_current_pages >= self.capacity):
    #             self.evict_recently_used()
    #             # self.number_current_pages += 1
    #         print("Entered: Page not in pool ...")

    #         self.number_current_pages += 1

    #         page_num_to_read = (page_num + number_columns) * -1
    #         frame = self.read_page_from_disk_tail(table_name, number_columns, page_num_to_read)
    #         print("frame is ",frame.key)
    #         if frame is None:
    #             print("Entered: create_new_page ...")
    #             frame = self.create_new_page(table_name, number_columns)            

    #         # self.frame_cache[frame.page.page_num] = frame
    #         # self.frame_cache.move_to_end(frame.page.page_num)
    #         self.frame_cache[page_num] = frame
    #         self.frame_cache.move_to_end(page_num)
            
    #         print(f"Placed page: {page_num} inside frame")
    #         print("number_current_pages:", self.number_current_pages, "capactiy:", self.capacity)

    #         return self.frame_cache[page_num]


    #         # raise KeyError(f"Invalid Page: {page_num}. Not in Queue.")
    #     else:
    #         print("Entered: frame IN pool ...")
    #         print("number_current_pages:", self.number_current_pages, "capactiy:", self.capacity)

    #         self.frame_cache.move_to_end(page_num)
    #         return self.frame_cache[page_num]