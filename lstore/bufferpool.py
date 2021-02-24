from lstore.config import *
from lstore.page import *

class BufferPool:

    def __init__(self):
        self.path = ""
        self.pool = {}
        self.capacity = BUFF_POOL_SIZE
        self.num_pages = 0
        self.total_db_pages = 0
        self.lru_queue = LRU_Queue()

    def assign_path(self, path):
        self.path = path


    def check_pool(self, key):
        #print(self.pool)
        if key in self.pool:
            return SUCCESS
        else:        
            return FAIL


    def print_pool(self):
        if len(self.pool) == 0:
            print("BufferPool is empty")

        for frame in self.pool.values():
            frame.print_page()


    def pin_page(self, page, table_name, num_col):
            self.enqueue_pages()
            self.evict_page()

            if (self.num_pages >= self.capacity):
                print("Cannot add page, all pages in buffer pool currently pinned")
                return FAIL

            frame = Frame(page.page_num, page, table_name, num_col)
            #print("frame")

            self.pool[page.page_num] = frame
            self.num_pages += 1
            return SUCCESS

    def grab_page(self, table_name, cur_page, num_col):

        if not self.check_pool(cur_page):
            #print("Pinning page!")
            new_page = Page(self.total_db_pages)
            self.total_db_pages += 1
            self.pin_page(new_page, table_name, num_col)
            #self.print_pool()

        page_to_return = self.pool[cur_page]
        return page_to_return
        #pass


    def read_page(self, table_name, page_num, num_cols):
        seek_offset = int(page_num/num_cols)
        seek_mult = PAGE_CAPACITY_IN_BYTES

        file_num = page_num % num_cols
        file_name = self.path + "/" + table_name + "_" + str(file_num) + ".bin"

        file = open(file_name, "rb")
        file.seek(seek_offset * seek_mult)
        data = file.read(seek_mult)

        #print(data)
        file.close()

        test_page = Page(page_num)
        test_page.data = data
        #test_page.display_internal_memory()
        
        pass
        
        
    def evict_page(self):
        if (self.num_pages < self.capacity):
            return FAIL

        if len(self.lru_queue.queue) == 0:
            print("No pages in LRU Queue, cannot evict")
            return FAIL

        lru_page = self.lru_queue.pop()

        if (lru_page == FAIL):
            return FAIL

        key = lru_page.key
        is_dirty = lru_page.is_dirty

        

        if (is_dirty):
            print("Evicting page ", key)
            lru_page.write_frame(self.path)
            #print("done_evict")
            self.pool.pop(key)
            self.num_pages -= 1
            return SUCCESS
        else:
            print("Evicting page ", key)
            self.pool.pop(key)
            self.num_pages -= 1
            return SUCCESS


    # Iterate through all pages in buffer pool and try to enqueue in LRU Queue
    def enqueue_pages(self):
        for frame in self.pool.values():
            if frame not in self.lru_queue.queue:
                self.lru_queue.add(frame)


class Frame:

    def __init__(self, page_num, page, table_name, num_col):
        self.key = page_num
        self.page = page
        self.table_name = table_name
        self.num_columns = num_col
        self.pin_count = 0
        self.is_dirty = False


    def write_value(self, val):
        self.page.write(val)
        self.is_dirty = True

    def write_frame(self, path):

        page_num = self.key
        num_col = self.num_columns #
        
        seek_offset = int(page_num/num_col)
        
        seek_mult = PAGE_CAPACITY_IN_BYTES
        

        file_num = page_num % num_col
        page  = self.page

        file_name = path + "/" + self.table_name + "_" + str(file_num) + ".bin"
        #print(file_name)

        
        file= open(file_name, "a+b") #binary

        file.seek(seek_offset * seek_mult)
        file.write(page.data)
        page.display_internal_memory()
        #print(page.data)
        file.close()
        
        self.is_dirty = False

        
        #page.display_internal_memory()


    def print_page(self):
        print(f"Page Identity: {self.key, self.page, self.table}")
        self.page.display_internal_memory()


class LRU_Queue:

    """
    Maintain a set of Frames, evict least recently used page. 
    """
    
    def __init__(self):
        self.queue = []

    def print_queue(self):
        for frame in self.queue:
            frame.print_page()

        print("\n")

    def add(self, frame):
        if frame.pin_count == 0:
            self.queue.append(frame)
            # print(self.queue)
            return SUCCESS
        else:
            print("Buffer page currently pinned, cannot add to LRU_Queue")
            return FAIL
        

    def pop(self):

        if len(self.queue) == 0:
            print("LRU_Queue is empty")
            return FAIL

        lru = self.queue.pop(0)

        while (lru.pin_count != 0 and len(self.queue) > 0):
            lru = self.queue.pop(0)

        if (lru.pin_count == 0):
            return lru
        else:
            print("All of LRU_Queue currently pinned")
            return FAIL


    def __len__(self):
        return len(self.queue)
