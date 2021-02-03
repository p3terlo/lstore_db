from lstore.page import *
from lstore.config import PAGE_CAPACITY_IN_BYTES, INTEGER_CAPACITY_IN_BYTES
from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


NULL_PTR = 0

CLEAN_BIT = 0
DIRTY_BIT = 1


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

        # Page directory should map RID to [page range, page, offset (AKA slot number)]
        self.page_directory = {}
        self.index = Index(self)

        # Added structures
        self.key_map = {}
        self.base_pages = []
        self.tail_pages = []


    def __merge(self):
        pass


    def create_base_pages(self):

        base_pages_full = False
        noBasePagesInTable = len(self.base_pages) == 0

        if not noBasePagesInTable:
            base_pages_full = self.base_pages[-1].is_full()

        # If initializing empty base pages for the first time, or if current set of base pages are full, create new set of base pages
        if ((noBasePagesInTable) or (base_pages_full)):
            for _ in range(self.num_columns):
                new_base_page = Page()
                self.base_pages.append(new_base_page)


    def create_tail_pages(self):
        num_tail_pages = len(self.tail_pages)
        num_records = len(self.key_map)

        noTailPagesInTable = num_tail_pages == 0
        records_covered = (num_tail_pages / self.num_columns) * PAGE_RANGE

        if ((noTailPagesInTable) or (num_records > records_covered)):
            for _ in range(self.num_columns):
                new_tail_page = Page()
                self.tail_pages.append(new_tail_page)


    def add(self, *columns):

        self.create_base_pages()
        self.create_tail_pages() 

        # Create record
        record_key = columns[self.key]
        epoch_time = time()
        rid = len(self.key_map)
        # Record_col stored as indirection, RID, timestamp, schema encoding 
        record_col = (NULL_PTR, rid, epoch_time, CLEAN_BIT)
        base_record = Record(rid, record_key, record_col)
        
        # Key -> record -> RID
        self.key_map[record_key] = base_record
        slots = PAGE_CAPACITY_IN_BYTES/INTEGER_CAPACITY_IN_BYTES
        slot_num = rid % slots #slot within page 
        page_range_num = int(rid / PAGE_RANGE)
        page_offset = int(rid / slots) #offset of which pages to access
        page_num = self.num_columns * page_offset #calculation of which page       
        
        
        for i in range(self.num_columns): #base 5 insertions to 5 pages
            self.base_pages[i + page_num].write(columns[i])

        directory = [page_range_num, page_num, slot_num]
        self.page_directory[rid] = directory

        
    def fetch(self, key):

        print("fetching record.....")
        
        record = self.key_map[key]

        #record.display()
        rid = record.rid

        page_locations = self.page_directory[rid]
        page_ids_belonging_to_rid = self.page_directory[rid]
  
        record_display = []

        slot_id = self.calculate_slot_number(rid)

        for page_id in page_ids_belonging_to_rid:
            record_display.append(self.base_pages[page_id].grab_slot(slot_id))

        print(record_display)
        print()
     
    
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
            
      
    def select(self, key, column, query_columns):

        #Get rid from key_map
        record = self.key_map[key]
        rid = record.rid

        #Gather page locations from page_directory
        page_ids_belonging_to_rid = self.page_directory[rid]

        #record_display: Array to hold record data
        #slot: slot within page

        record_display = [] #[91469300,1,2,3,4]
        slot = self.calculate_slot_number(rid)

        #Only grab queried records from pages EX: [1,1,1,1,1]
        for page_id in page_ids_belonging_to_rid:
            if (query_columns[page_id % self.num_columns] == 0): 
                continue
            record_display.append(self.base_pages[page_id].grab_slot(slot))

        #Create temp record
        #Placeholder Talked w/ Alvin about this and still deciding on what design to go with. For now, will query data,
        #and place it into new record which is returned instead of #returning record on file.
        return_array = []
        temp_record = Record(rid, key, record_display)
        return_array.append(temp_record)
        
        return return_array

    # Implemented with tail page and page range
    def update(self, key, *columns):

        self.create_tail_pages() 

        # Create new record
        new_rid = len(self.key_map)
        new_time = time()
        new_record_col = (NULL_PTR, new_rid, new_time, CLEAN_BIT)
        new_record = Record(new_rid, key, new_record_col)

        # Update indirection columns
        base_record = self.key_map[key]
        latest_record = base_record.columns[INDIRECTION_COLUMN]
        # If base record had previous updates
        if latest_record != NULL_PTR:
            # Point new record's indirection to previous most recent record
            new_record_col = (latest_record, new_rid, new_time, CLEAN_BIT)
        # If base record had no updates
        else:
            # Point new record's indirection to base record 
            new_record_col = (base_record.rid, new_rid, new_time, CLEAN_BIT)

        # Set base record's indirection column to new record's RID
        base_record.columns[INDIRECTION_COLUMN] = new_rid

        # Write new record to tail page
        page_offset = int(new_rid / PAGE_RANGE)
        page_num = self.num_columns * page_offset

        for i in range(self.num_columns):
            self.tail_pages[page_num + i].write(columns[i])

        # Update page directory
        slot_num = self.tail_pages[page_num].next_empty_slot()
        directory = [page_offset, page_num, slot_num]
        self.page_directory[new_rid] = directory


    def sum(self, start_range, end_range, col_index_to_add):
        total = 0

        for key in self.key_map:
            if key < start_range or key > end_range:
                continue

            #Grab record from key_map
            #Grab rid from record object
            record = self.key_map[key]
            rid = record.rid

            #Grab page locations from page_directory
            #Grab single page we care about from page locations
            page_ids_belonging_to_rid = self.page_directory[rid]
            page_to_add = page_ids_belonging_to_rid[col_index_to_add]

            #Get slot number from rid
            slot = self.calculate_slot_number(rid)               
            total = total + self.base_pages[page_to_add].grab_slot(slot)

        return total
