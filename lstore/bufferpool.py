from lstore.config import *

class BufferPool:

    def __init__(self):
        self.pool = {}
        self.capacity = BUFF_POOL_SIZE
        self.num_pages = 0
        self.total_db_pages = 0
        self.lru_queue = LRU_Queue()


    def check_pool(self, key):
        # print(self.pool)
        if key in self.pool:
            return SUCCESS
        else:        
            return FAIL


    def print_pool(self):
        if len(self.pool) == 0:
            print("BufferPool is empty")

        for frame in self.pool.values():
            frame.print_page()


    def pin_page(self, page, table_name):
            self.enqueue_pages()
            self.evict_page()

            if (self.num_pages >= self.capacity):
                print("Cannot add page, all pages in buffer pool currently pinned")
                return FAIL

            frame = Frame(page.page_num, page, table_name)

            self.pool[page.page_num] = frame
            self.num_pages += 1
            return SUCCESS


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
            # Write page to memory, then pop
            pass
            # return SUCCESS
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

    def __init__(self, page_num, page, table):
        self.key = page_num
        self.page = page
        self.table = table
        self.pin_count = 0
        self.is_dirty = False

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