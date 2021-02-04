from lstore.page import *
from lstore.config import *
from lstore.index import Index
import time

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

        self.tail_rid = MAX_INT


    def __merge(self):
        pass


    @staticmethod
    def calculate_base_page_numbers(num_columns, rid):
        '''
        Maps a given RID -> corresponding slot_id within a page according to the 
        PAGE_CAPACITY_IN_BYTES and INTEGER_CAPACITY_IN_BYTES constraints.

        Additionally gives the beginning page_id which a record belongs to depending on
        the num_columns given.
        '''
        # We subtract 1 from RID because first RID starts at 1
        output = {}

        slots_per_page = int(PAGE_CAPACITY_IN_BYTES / INTEGER_CAPACITY_IN_BYTES)
        output[SLOT_NUM_COL] = (rid - 1) % slots_per_page

        page_offset = int((rid - 1) / slots_per_page) 
        output[PAGE_NUM_COL] = (num_columns) * page_offset

        page_range_num = int((rid - 1) / PAGE_RANGE)
        output[PAGE_RANGE_COL] = page_range_num

        return output

    @staticmethod
    def calculate_tail_page_numbers(num_columns, rid):
        output = {}

        slots_per_page = int(PAGE_CAPACITY_IN_BYTES / INTEGER_CAPACITY_IN_BYTES)
        output[SLOT_NUM_COL] = (MAX_INT - rid) % slots_per_page

        # print("SPP", slots_per_page,"output slotnum", output[SLOT_NUM_COL])

        # page_offset = int((MAX_INT - rid) / PAGE_RANGE) 
        page_offset = int((MAX_INT - rid) / slots_per_page) 

        output[PAGE_NUM_COL] = num_columns * page_offset

        # print("pageoffset", page_offset, "output pagenum",output[PAGE_NUM_COL])

        # output[PAGE_RANGE_COL] = page_offset
        output[PAGE_RANGE_COL] = int((MAX_INT - rid) / PAGE_RANGE) 


        return output


    def create_base_pages(self):
        base_pages_full = False
        noBasePagesInTable = len(self.base_pages) == 0

        if not noBasePagesInTable:
            base_pages_full = self.base_pages[-1].is_full()

        # If initializing empty base pages for the first time, or if current set of base pages are full, create new set of base pages
        if ((noBasePagesInTable) or (base_pages_full)):
            for _ in range(self.num_columns + NUM_DEFAULT_COLUMNS):
                new_base_page = Page()
                self.base_pages.append(new_base_page)


    def create_tail_pages(self):
        num_tail_pages = len(self.tail_pages)
        num_records = len(self.key_map) - 1
        

        noTailPagesInTable = num_tail_pages == 0
        records_covered = (num_tail_pages / self.num_columns) * PAGE_RANGE

        if not noTailPagesInTable:
            tail_pages_full = self.base_pages[-1].is_full()


        if ((noTailPagesInTable) or (num_records > records_covered) or (tail_pages_full)):
            for _ in range(self.num_columns + NUM_DEFAULT_COLUMNS):
                new_tail_page = Page()
                self.tail_pages.append(new_tail_page)


    def add(self, *columns):

        self.create_base_pages()
        self.create_tail_pages()

        # Create record
        rid = len(self.key_map) + 1
        record_key = columns[self.key]
        milliseconds = int(round(time.time() * 1000))
        schema = 0

        # Record_col stored as RID, indirection, timestamp, schema encoding
        record_col = [rid, NULL_PTR, milliseconds, schema]

        for column in columns:
            record_col.append(column)

        base_record = Record(rid, record_key, record_col)
        
        # Key -> record -> RID
        self.key_map[record_key] = base_record

        page_dict = self.calculate_base_page_numbers(self.num_columns + NUM_DEFAULT_COLUMNS, rid)
        slot_num = page_dict[SLOT_NUM_COL]
        starting_page_num = page_dict[PAGE_NUM_COL]
        page_range_num = page_dict[PAGE_RANGE_COL]
        
        for i in range(len(record_col)):
            self.base_pages[starting_page_num + i].write(record_col[i])

        directory = [page_range_num, starting_page_num, slot_num]
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

        
    def display_pages(self):
        for page in self.base_pages:
            page.display_internal_memory()
            
      
    def select(self, key, column, query_columns):

        #Get rid from key_map
        record = self.key_map[key]
        rid = record.rid

        #Gather page locations from page_directory
        page_location = self.page_directory[rid]

        base_record = []
        record_display = []

        slot = page_location[SLOT_NUM_COL]
        start_page = page_location[PAGE_NUM_COL]
        first_column_page = start_page + NUM_DEFAULT_COLUMNS
        last_page = start_page + NUM_DEFAULT_COLUMNS + self.num_columns
        indirection = self.base_pages[start_page + INDIRECTION_COLUMN].grab_slot(slot)

        tail_update_index = []

        # Pull entire base record
        # for page in range(start_page, last_page):
        for page in range(9):
            # base_record.append(self.base_pages[start_page + page%(NUM_DEFAULT_COLUMNS+self.num_columns)].grab_slot(slot))
            base_record.append(self.base_pages[start_page + page].grab_slot(slot))


        # print("BASE RECORD:",base_record)

        # For base records with no updates, take subset of base_record
        if (indirection == NULL_PTR):
            for page in range(NUM_DEFAULT_COLUMNS, self.num_columns + NUM_DEFAULT_COLUMNS):
                if (query_columns[page - NUM_DEFAULT_COLUMNS] == 1):
                    record_display.append(base_record[page])

        # If record has updates, go to most recent update
        else:
            page_location = self.page_directory[indirection]
            start_page = page_location[PAGE_NUM_COL]
            slot_num = page_location[SLOT_NUM_COL]
            
            schema = self.tail_pages[start_page + SCHEMA_ENCODING_COLUMN].grab_slot(slot_num)
            schema = str(schema)
            schema = "0000" + schema
            schema = schema[-5:]

            for i in range(len(schema)):
                if schema[i] == "1":
                    # tail_update_index.append(self.num_columns - (len(schema) - i))
                    tail_update_index.append(i)

            for page in tail_update_index:
                base_record[NUM_DEFAULT_COLUMNS + page] = self.tail_pages[start_page + NUM_DEFAULT_COLUMNS + page].grab_slot(slot_num)

            for page in range(NUM_DEFAULT_COLUMNS, self.num_columns + NUM_DEFAULT_COLUMNS):
                if (query_columns[page - NUM_DEFAULT_COLUMNS] == 1):
                    record_display.append(base_record[page])

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
        new_rid = self.tail_rid
        # print(f"new rid: {new_rid}")
        new_time = int(round(time.time() * 1000))
        schema = ""
        for column in columns:
            if column == None:
                schema += "0"
            else:
                schema += "1"
        schema = int(schema)
        # print(schema)

        new_record_col = [new_rid, NULL_PTR, new_time, schema]

        for column in columns:
            new_record_col.append(column)

        # print(new_record_col)

        new_record = Record(new_rid, key, new_record_col)

        # Update indirection columns
        base_record = self.key_map[key]
        latest_record = base_record.columns[INDIRECTION_COLUMN]
        # If base record had previous updates
        if latest_record != NULL_PTR:
            # Point new record's indirection to previous most recent record
            new_record_col[INDIRECTION_COLUMN] = latest_record

            prevUpdatePages = self.page_directory[latest_record]
            first_schema = new_record_col[SCHEMA_ENCODING_COLUMN]
            prev_schema = self.tail_pages[prevUpdatePages[PAGE_NUM_COL] + SCHEMA_ENCODING_COLUMN].grab_slot(prevUpdatePages[SLOT_NUM_COL])

            first_schema_string = "00000" + str(first_schema)
            first_schema_string = first_schema_string[-5:]
            prev_schema_string = "00000" + str(prev_schema)
            prev_schema_string = prev_schema_string[-5:]

            new_schema_string = ""

            for i in range(5):
                if first_schema_string[i] == "1" or prev_schema_string[i] == "1":
                    new_schema_string += "1"

                else: 
                    new_schema_string += "0"

            schemaTemp = str(new_record_col[SCHEMA_ENCODING_COLUMN])
            schemaTemp = "00000" + schemaTemp
            schemaTemp = schemaTemp[-5:]

            for i in range(len(prev_schema_string)):
                if prev_schema_string[i] == "1" and first_schema_string[i] != "1":
                    new_record_col[NUM_DEFAULT_COLUMNS+i] = self.tail_pages[prevUpdatePages[PAGE_NUM_COL]+NUM_DEFAULT_COLUMNS+i].grab_slot(prevUpdatePages[SLOT_NUM_COL])
                    
            new_record_col[SCHEMA_ENCODING_COLUMN] = int(new_schema_string)
 
        # If base record had no updates
        else:
            # Point new record's indirection to base record 
            new_record_col[INDIRECTION_COLUMN] = base_record.rid

        # Set base record's indirection column to new record's RID
        base_page_location = self.page_directory[base_record.rid]
        base_page_num = base_page_location[PAGE_NUM_COL]
        base_page_slot = base_page_location[SLOT_NUM_COL]
        self.base_pages[base_page_num + INDIRECTION_COLUMN].update(new_rid, base_page_slot)
        base_record.columns[INDIRECTION_COLUMN] = new_rid

        # Write new record to tail page
        page_dict = self.calculate_tail_page_numbers(self.num_columns + NUM_DEFAULT_COLUMNS, new_rid)
        page_range_num = page_dict[PAGE_RANGE_COL]
        page_num = page_dict[PAGE_NUM_COL]
        slot_num = page_dict[SLOT_NUM_COL]

        for i in range(len(new_record_col)):
            if (new_record_col[i] != None):
                # print("slot_num:", slot_num, "Page_Num",page_num)
                self.tail_pages[page_num + i].write(new_record_col[i])
            else:
                self.tail_pages[page_num+i].write(0)

        # Update page directory
        directory = [page_range_num, page_num, slot_num]
        self.page_directory[new_rid] = directory

        # Decrement tail rid
        self.tail_rid -= 1


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
            pages = self.page_directory[rid] 
            #page range num, starting page num, slot num

            indirection = self.base_pages[pages[PAGE_NUM_COL]+INDIRECTION_COLUMN].grab_slot(pages[SLOT_NUM_COL])
            # print(indirection)
            if indirection != NULL_PTR:
                # print("going into if")
                pages_tail = self.page_directory[indirection]

                schema = self.tail_pages[pages_tail[PAGE_NUM_COL]+SCHEMA_ENCODING_COLUMN].grab_slot(pages_tail[SLOT_NUM_COL])
                schema_string = "00000" + str(schema)
                schema_string = schema_string[-5:]

                if schema_string[col_index_to_add] == "1":
                    val_to_add = self.tail_pages[pages_tail[PAGE_NUM_COL] + NUM_DEFAULT_COLUMNS + col_index_to_add].grab_slot(pages_tail[SLOT_NUM_COL])
                    total = total + val_to_add
                    # print(total)
                else:
                    total = total + self.base_pages[pages[PAGE_NUM_COL] + NUM_DEFAULT_COLUMNS + col_index_to_add].grab_slot(pages[SLOT_NUM_COL])
            else:
                total = total + self.base_pages[pages[PAGE_NUM_COL] + NUM_DEFAULT_COLUMNS + col_index_to_add].grab_slot(pages[SLOT_NUM_COL])
                # print("going into else")

            # page_to_add = pages[col_index_to_add]
            # slot = self.calculate_slot_number(rid)               
            # total = total + self.base_pages[page_to_add].grab_slot(slot)

        return total

    def delete(self, key):
        try:
            record = self.key_map[key]
            record.rid = None
            return True
        except:
            return False

        #FIXME Need to check if record.rid = none in other functions now so there are no errors.