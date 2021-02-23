from lstore.config import *

class BufferPool:

    def __init__(self):
        self.pool = {}
        self.capacity = BUFF_POOL_SIZE
        self.num_pages = 0
        self.lru_queue = LRU_Queue()

    def check_pool(self, key, table):
        if key in self.pool[table].pages:
            return SUCCESS
        else:        
            return FAIL

    def add(self, buffer_page):
        self.enqueue_pages()
        self.evict()

        if (self.num_pages >= self.capacity):
            print("Cannot add page, all pages in buffer pool currently pinned")
            return FAIL

        key = buffer_page.key
        table = buffer_page.table

        self.pool[table].pages[key] = buffer_page
        self.num_pages += 1
        return SUCCESS

    def evict(self):
        if (self.num_pages < self.capacity):
            print("Buffer Pool still has room, no need to evict")
            return FAIL

        if len(self.lru_queue.queue) == 0:
            print("No pages in LRU Queue")
            return FAIL

        lru_page = self.lru_queue.pop()

        if (lru_page == FAIL):
            print("Evict error")
            return FAIL

        key = lru_page.key
        table = lru_page.table
        is_dirty = lru_page.is_dirty

        if (is_dirty):
            # Write page to memory, then pop
            return SUCCESS
        else:
            self.pool[table].pages.pop(key)
            self.num_pages -= 1
            return SUCCESS

    # Iterate through all pages in buffer pool and try to enqueue in LRU Queue
    def enqueue_pages(self):
        for table in self.pool.values():
            for page_num, buffer_page in table.items():
                self.lru_queue.add(buffer_page)


class BufferTable:

    def __init__(self, name, table):
        self.name = name
        self.table = table
        self.pages = {}


class BufferPage:

    def __init__(self, page_num, table, page):
        self.key = page_num
        self.table = table
        self.page = page
        self.pin_count = 0
        self.is_dirty = False


class LRU_Queue:

    def __init__(self):
        self.queue = []

    def add(self, buffer_page):
        if buffer_page.pin_count == 0:
            self.queue.append(buffer_page)
            return SUCCESS
        else:
            # print("Buffer page currently pinned, cannot add to LRU_Queue")
            return FAIL

    def pop(self):
        if len(self.queue) == 0:
            print("LRU_Queue is empty")
            return FAIL

        lru = self.queue.pop(0)

        while (lru.pin_count != 0):
            lru = self.queue.pop(0)

        if (lru.pin_count == 0):
            return lru
        else:
            print("All of LRU_Queue currently pinned")
            return FAIL