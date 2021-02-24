from lstore.config import *

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
        if self.num_pages == 0:
            print("BufferPool is empty")

        for bp in self.pool.values():
            bp.print_page()


    def add(self, buffer_page):
        self.enqueue_pages()
        self.evict()

        if (self.num_pages >= self.capacity):
            print("Cannot add page, all pages in buffer pool currently pinned")
            return FAIL

        key = buffer_page.key

        self.pool[key] = buffer_page
        self.num_pages += 1
        return SUCCESS

    def evict(self):
        if (self.num_pages < self.capacity):
            print("Buffer Pool still has room, no need to evict")
            return FAIL

        if len(self.lru_queue.queue) == 0:
            print("No pages in LRU Queue, cannot evict")
            return FAIL

        # while len(self.lru_queue.queue) > 0:
        lru_page = self.lru_queue.pop()

        if (lru_page == FAIL):
            # print("Evict error")
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
        # for table in self.pool.values():
        #     for page_num, buffer_page in table.items():
        #         self.lru_queue.add(buffer_page)

        for bp in self.pool.values():
            if bp not in self.lru_queue.queue:
                self.lru_queue.add(bp)


class BufferPage:

    def __init__(self, page_num, page, table):
        self.key = page_num
        self.page = page
        self.table = table
        self.pin_count = 0
        self.is_dirty = False

    def print_page(self):
        print(self.key, self.page, self.table)


class LRU_Queue:

    """
    Maintain a set of Frames, evict least recently used page. 
    """
    
    def __init__(self):
        self.queue = []

    def print_queue(self):
        for bp in self.queue:
            bp.print_page()

        print("\n")

    def add(self, buffer_page):
        if buffer_page.pin_count == 0:
            self.queue.append(buffer_page)
            # print(self.queue)
            return SUCCESS
        else:
            print("Buffer page currently pinned, cannot add to LRU_Queue")
            return FAIL
        

    def pop(self):

        # self.print_queue()

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