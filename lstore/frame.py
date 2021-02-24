class Frame:
    """
    Container for holding pages in bufferpool.
    """

    def __init__(self, page_num, page, table):
        self.key = page_num
        self.page = page
        self.table = table
        self.outstanding_transactions = 0
        self.is_dirty = False


    def pin_page(self):
        self.outstanding_transactions += 1


    def unpin_page(self):
        if self.outstanding_transactions > 0:
            self.outstanding_transactions -= 1


    def make_dirty(self):
        self.is_dirty = True

    
    def clean_page(self):
        self.is_dirty = False


    def print_page(self):
        print(f"Page Identity: {self.key, self.page, self.table}")
        self.page.display_internal_memory()