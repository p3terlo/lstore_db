from lstore.config import *
from lstore.frame import Frame
# from lstore.lru import LRU_Queue


class BufferPool:

    def __init__(self):
        self.pool = {}
        self.capacity = BUFF_POOL_SIZE
        self.num_pages = 0
        self.total_db_pages = 0
        self.lru_queue = LRU_Queue()


    def check_pool(self, key):

        if key in self.pool:
            return SUCCESS
        else:        
            return FAIL


    def print_pool(self):
        if len(self.pool) == 0:
            print("BufferPool is empty")

        for frame in self.pool.values():
            frame.print_page()


    def add_page(self, page, table_name):
        self.enqueue_pages()
        self.evict_page()

        if (self.num_pages >= self.capacity):
            print("Cannot add page, all pages in buffer pool currently pinned")
            return FAIL

        frame = Frame(page.page_num, page, table_name)

        self.pool[page.page_num] = frame
        self.num_pages += 1
        return SUCCESS


    def pin_page(self, page_num):
        frame = self.pool[page_num]
        frame.increment_pin()


    def unpin_page(self, page_num):
        frame = self.pool[page_num]
        frame.decrement_pin()


    def evict_page(self):

        if (self.num_pages < self.capacity):
            return FAIL

        if len(self.lru_queue.queue) == 0:
            print("No pages in LRU Queue, cannot evict")
            return FAIL

        lru_frame = self.lru_queue.pop()

        if (lru_frame == FAIL):
            return FAIL

        key = lru_frame.key
        is_dirty = lru_frame.is_dirty

        if (is_dirty):
            # Write page to memory, then pop
            lru_frame.clean_page()
            pass
            # return SUCCESS


        else:
            print("Evicting page ", key)
            self.pool.pop(key)
            self.num_pages -= 1
            return SUCCESS


    # def read_page(self, page_num, num_cols):
    #     seek_offset = int(key / num_cols)
    #     seek_mult = PAGE_CAPACITY_IN_BYTES
    #     col = key % num_cols 
    #     file_name = self.path + "/" + self.name + "_" + str(col) + ".bin"

    #     try:
    #         file = open(file_name, "rb")
    #     except:
    #         print(f"Page {page_num} doesn't exist on disk")
    #         return FAIL

    #     file.seek(seek_offset * seek_mult)
    #     data = file.read(8 * seek_mult) #read one bit at a time hence times 8
    #     file.close()

    #     page = Page(page_num)
    #     page.num_records = num_records
    #     page.data = data
    #     return page


    # Iterate through all pages in buffer pool and try to enqueue in LRU Queue
    def enqueue_pages(self):
        for frame in self.pool.values():
            if frame not in self.lru_queue.queue:
                self.lru_queue.add(frame)


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