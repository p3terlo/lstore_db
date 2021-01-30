from lstore.page import Page, PAGE_CAPACITY_IN_BYTES, INTEGER_CAPACITY_IN_BYTES

from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def display(self):
        print()
        print("Record vals:")
        print("Rid: %d with key %d" % (self.rid ,self.key))


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)

        #added structures
        self.base_pages = [] 
        self.key_map = {}
        self.tail_pages = []
        pass

    def __merge(self):
        pass


    def __initialize_empty_base_pages(self):
        noBasePagesInTable = len(self.base_pages) == 0

        if (noBasePagesInTable):
            for _ in range(self.num_columns):
                new_base_page = Page()
                #new_tail_page = Page()
                self.base_pages.append(new_base_page)
                
        elif self.base_pages[-1].is_full(): # adding more base pages if last page is full
            for _ in range(self.num_columns):
                new_base_page = Page()
                #new_tail_page = Page()
                self.base_pages.append(new_base_page)


    def add(self, *columns): #can divide sub sections if needed

        self.__initialize_empty_base_pages()

        #defaulting record vals
        record_key = columns[self.key]
        epoch_time = time()
        rid = len(self.key_map)

        # FIXME will edit schema later
        record_col = (0, rid, epoch_time, 0)
        new_record = Record(rid, record_key, record_col)
        
        # FIXME change key_map to index
        self.key_map[record_key] = new_record #adding to mapping key -> record -> rid
        #new_record.display()

        start_page_id = self.__rid_to_page_id(self.num_columns, rid)
        
        directory = []

        for col_num in range(self.num_columns): #base 5 insertions to 5 pages
            page_id = col_num + start_page_id
            self.base_pages[page_id].write(columns[col_num])
            directory.append(page_id)

        #assuming rid is the slot number
        self.page_directory[rid] = directory
        
        
    def fetch(self, key):

        print("fetching record.....")
        
        record = self.key_map[key]

        #record.display()
        rid = record.rid

        page_ids_belonging_to_rid = self.page_directory[rid]

        record_display = []

        slot_id = self.calculate_slot_number(rid)

        for page_id in page_ids_belonging_to_rid:
            record_display.append(self.base_pages[page_id].grab_slot(slot_id))

        print(record_display)
        

    @staticmethod
    def calculate_slot_number(rid):
        slots_per_page = int(PAGE_CAPACITY_IN_BYTES / INTEGER_CAPACITY_IN_BYTES)
        slot_id = rid % slots_per_page
        return slot_id


    @staticmethod
    def __rid_to_page_id(num_columns, rid):
        '''
        Given the # of Columns of a base page. 
        We map a RID -> page_id
        '''
        slots_per_page = PAGE_CAPACITY_IN_BYTES / INTEGER_CAPACITY_IN_BYTES
    
        # Floor Operation with Type Conversion
        page_offset = int(rid / slots_per_page) #offset of which pages to access
        page_id = num_columns * page_offset #calculation of which page

        return page_id


    def display_pages(self):
        for page in self.base_pages:
            page.display_internal_memory()


        
 
