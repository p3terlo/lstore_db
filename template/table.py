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
        self.columns = columns

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
        record_display = []
        slot = self.slot_num(rid)

        #Grab records from pages
        #only those that are being queried EX: [1,1,1,1,1]
        for i in page_locations:
            if(query_columns[i%self.num_columns] == 0): 
                continue
            record_display.append(self.base_pages[i].grab_slot(slot))

        #Create temp record
        #This is a placeholder implementation, need to talk about
        #design as rn it does not seem that current 
        #record.columns design would work with m1_tester
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

        #record_display: Array to hold record data
        #slot: slot within page
        slot = self.slot_num(rid)

        #Grab records from pages
        #only those that are being queried EX: [1,1,1,1,1]
        for i in page_locations:
            print("i is",i)
            self.base_pages[i].update(columns[i%5], slot)
            print(self.base_pages[i].data[slot])


        pass


        
 
