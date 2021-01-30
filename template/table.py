from template.page import *
from template.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns #[914, 1,2,3,4]

    def display(self):
        print()
        print("Record vals:")
        print("Rid: %d with key %d" % (self.rid ,self.key))
        #print(self.columns)
        #print()

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

    def add(self, *columns): #can divide sub sections if needed

        #initializtin pages
        if (len(self.base_pages) == 0):
            for i in range(self.num_columns):
                new_base_page = Page()
                #new_tail_page = Page()
                self.base_pages.append(new_base_page)
                
        elif self.base_pages[-1].is_full(): #adding more base pages if full
            #print("is FULL")
            for i in range(self.num_columns):
                new_base_page = Page()
                #new_tail_page = Page()
                self.base_pages.append(new_base_page)

        

        #defaulting record vals
        record_key = columns[self.key]
        epoch_time = time()
        rid = len(self.key_map)
        record_col = (0, rid, epoch_time, 0) #will edit schema later
        new_record = Record(rid, record_key, record_col)
        
        self.key_map[record_key] = new_record #adding to mapping key -> record -> rid
        #new_record.display()
        #self.base_pages[0].write(columns[0])

        slots = Page().get_size()/8
        
        page_offset = int(rid / slots) #offset of which pages to access
        
        slot_num = rid % slots #slot within page
        page_num = self.num_columns * page_offset #calculation of which page

        #print(rid)
       # print(slots)
        
        
        directory = []

        for i in range(self.num_columns): #base 5 insertions to 5 pages
            self.base_pages[i + page_num].write(columns[i])
            directory.append(i+page_num)
      
        #for i in self.base_pages:
        #   i.display_mem()

        #assuming rid is the slot number
        self.page_directory[rid] = directory
        #print(directory)
        #print(self.page_directory)

        
    def fetch(self, key):

        print("fetching record.....")
        
        record = self.key_map[key]

        #record.display()
        rid = record.rid

        page_locations = self.page_directory[rid]

        record_display = []

        slot = self.slot_num(rid)

        for i in page_locations:
            record_display.append(self.base_pages[i].grab_slot(slot))

        print(record_display)
        print()
        

    def slot_num(self, rid):
        num_slots = int(Page().get_size()/8)
        slot_val = rid % num_slots

        return slot_val
        
    def display_pages(self):
        for i in self.base_pages:
            i.display_mem()

    def select(self, key, column, query_columns):
        # print("Key:", key)
        # print("Column:", column)
        # print("Query_Columns:", query_columns)

        #Get rid from key_map
        record = self.key_map[key]
        rid = record.rid

        #Gather page locations from page_directory
        page_locations = self.page_directory[rid]

        #record_display: Array to hold record data
        #slot: slot within page
        record_display = [] #914..,1,2,3,4
        slot = self.slot_num(rid)

        #0, rid, timestamp, 0

        #914..,1,2,3,4


        #Grab records from pages
        #only those that are being queried EX: [1,1,1,1,1]
        for i in page_locations:
            if(query_columns[i%self.num_columns] == 0): 
                continue
            record_display.append(self.base_pages[i].grab_slot(slot))

        #Create temp record
        #Placeholder implementation. Talked w/ Alvin about this and still #deciding on what design to go with. For now, will query data,
        #and place it into new record which is returned instead of #returning record on file.
        return_array = []
        temp_record = Record(rid, key, record_display)
        return_array.append(temp_record)
        print("record_display:",record_display)

        return return_array

    def update(self, key, *columns):
        print("key:", key)
        print("columns:", columns)

        #Get rid from key_map
        record = self.key_map[key]
        rid = record.rid

        #Gather page locations from page_directory
        page_locations = self.page_directory[rid]

        #slot: slot within page
        slot = self.slot_num(rid)

        #Grab records from pages
        #Only grabs the requested columns EX: [1,1,1,1,1]
        for i in page_locations:
            self.base_pages[i].update(columns[i%5], slot)

        pass

    def sum(self, start_range, end_range, col_index_to_add):
        total = 0

        for key in self.key_map:
            if key < start_range:
                continue
            if key > end_range:
                continue

            #Grab record from key_map
            #Grab rid from record object
            # print(key)
            record = self.key_map[key]
            rid = record.rid

            #Grab page locations from page_directory
            #Grab single page we care about from page locations
            page_locations = self.page_directory[rid]
            page_to_add = page_locations[col_index_to_add]

            #Get slot number from rid
            slot = self.slot_num(rid)               

            #print(self.base_pages[page_to_add].data)
            #print(self.base_pages[page_to_add].data[(slot*8)+7])
            total = total + self.base_pages[page_to_add].data[(slot*8)+7]

        return total
