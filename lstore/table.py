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
        self.base_pages = []
        self.tail_pages = []

        self.base_rid = 1
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

        page_offset = int((MAX_INT - rid) / slots_per_page) 

        output[PAGE_NUM_COL] = num_columns * page_offset

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
        num_records = self.base_rid - 1

        noTailPagesInTable = num_tail_pages == 0
        records_covered = (num_tail_pages / self.num_columns) * PAGE_RANGE

        if not noTailPagesInTable:
            tail_pages_full = self.tail_pages[-1].is_full()

        if ((noTailPagesInTable) or (num_records > records_covered) or (tail_pages_full)):
            for _ in range(self.num_columns + NUM_DEFAULT_COLUMNS):
                new_tail_page = Page()
                self.tail_pages.append(new_tail_page)

    def add(self, *columns):
        self.create_base_pages()
        self.create_tail_pages()

        # Create record
        rid = self.base_rid
        record_key = columns[self.key]
        milliseconds = int(round(time.time() * 1000))
        schema = 0

        # Record_col stored as RID, indirection, timestamp, schema encoding
        record_col = [rid, NULL_PTR, milliseconds, schema]

        for column in columns:
            record_col.append(column)

        base_record = Record(rid, record_key, record_col)
        
        # Key -> record -> RID
        self.index.insert(record_key, base_record)

        page_dict = self.calculate_base_page_numbers(self.num_columns + NUM_DEFAULT_COLUMNS, rid)
        slot_num = page_dict[SLOT_NUM_COL]
        starting_page_num = page_dict[PAGE_NUM_COL]
        page_range_num = page_dict[PAGE_RANGE_COL]
        
        for i in range(len(record_col)):
            self.base_pages[starting_page_num + i].write(record_col[i])

        directory = [page_range_num, starting_page_num, slot_num]
        self.page_directory[rid] = directory

        self.base_rid += 1

        
    def fetch_page(self, key):
        
        record = self.index.locate(column = 0, value = key)[0]
        rid = record.rid

        page_locations = self.page_directory[rid]
        page_id = page_locations[PAGE_NUM_COL]

        return page_id

        
    # def display_pages(self):
    #     for page in self.base_pages:
    #         page.display_internal_memory()
            
      
    def select(self, key, column, query_columns):
        record = self.index.locate(column = 0, value = key)[0]

        rid = record.rid

        # Check if record was deleted
        if rid == None:
            return [False]

        # Gather page locations from page_directory
        page_location = self.page_directory[rid]

        # Array to hold records
        base_record = []
        record_display = []

        # Get slot and page numbers
        slot = page_location[SLOT_NUM_COL]
        start_page = page_location[PAGE_NUM_COL]
        first_column_page = start_page + NUM_DEFAULT_COLUMNS
        last_page = start_page + NUM_DEFAULT_COLUMNS + self.num_columns
        indirection = self.base_pages[start_page + INDIRECTION_COLUMN].grab_slot(slot)

        tail_update_index = []

        # Pull entire base record to base_record array
        for page in range(NUM_DEFAULT_COLUMNS + self.num_columns):
            base_record.append(self.base_pages[start_page + page].grab_slot(slot))

        # For base records with no updates, take requested subset of base_record
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
            leadingZeros = "0" * self.num_columns
            schema = leadingZeros + schema
            num = self.num_columns * -1
            schema = schema[num:]

            # If column is requested, pull from tail page
            for i in range(len(schema)):
                if schema[i] == "1":
                    # tail_update_index.append(i)
                    base_record[NUM_DEFAULT_COLUMNS + i] = self.tail_pages[start_page + NUM_DEFAULT_COLUMNS + i].grab_slot(slot_num)

            #for page in tail_update_index:
            #    base_record[NUM_DEFAULT_COLUMNS + page] = self.tail_pages[start_page + NUM_DEFAULT_COLUMNS + page].grab_slot(slot_num)

            for page in range(NUM_DEFAULT_COLUMNS, self.num_columns + NUM_DEFAULT_COLUMNS):
                if (query_columns[page - NUM_DEFAULT_COLUMNS] == 1):
                    record_display.append(base_record[page])

        #Create temp record
        return_array = []
        temp_record = Record(rid, key, record_display)
        return_array.append(temp_record)
        
        return return_array


    # Implemented with tail page and page range
    def update(self, key, *columns):
        self.create_tail_pages() 

        # Create new record
        new_rid = self.tail_rid
        new_time = int(round(time.time() * 1000))
        schema = ""
        for column in columns:
            if column == None:
                schema += "0"
            else:
                schema += "1"
        schema = int(schema)

        new_record_col = [new_rid, NULL_PTR, new_time, schema]

        for column in columns:
            new_record_col.append(column)

        new_record = Record(new_rid, key, new_record_col)

        # Update indirection columns
        base_record = self.index.locate(column = 0, value = key)[0]

        # Record not found
        if base_record == -1:
            return False

        latest_record = base_record.columns[INDIRECTION_COLUMN]

        # If base record had previous updates
        if latest_record != NULL_PTR:
            # Point new record's indirection to previous most recent record
            new_record_col[INDIRECTION_COLUMN] = latest_record

            # Get both prev and current schema we are creating
            prevUpdatePages = self.page_directory[latest_record]
            current_schema = new_record_col[SCHEMA_ENCODING_COLUMN]
            prev_schema = self.tail_pages[prevUpdatePages[PAGE_NUM_COL] + SCHEMA_ENCODING_COLUMN].grab_slot(prevUpdatePages[SLOT_NUM_COL])

            # Convert from int to useable string format EX: 01 -> "00001"
            leadingZeros = "0" * self.num_columns
            current_schema_string = leadingZeros + str(current_schema)
            num = self.num_columns * -1
            current_schema_string = current_schema_string[num:]
            prev_schema_string = leadingZeros + str(prev_schema)
            prev_schema_string = prev_schema_string[num:]

            new_schema_string = ""

            #Combine prev and current schema EX: 010 + 100 = 110
            for i in range(self.num_columns):
                if current_schema_string[i] == "1" or prev_schema_string[i] == "1":
                    new_schema_string += "1"
                else: 
                    new_schema_string += "0"

            # Grab updates from previous updates, don't grab new updates or will overwrite them
            for i in range(len(prev_schema_string)):
                if prev_schema_string[i] == "1" and current_schema_string[i] != "1":
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
                self.tail_pages[page_num + i].write(new_record_col[i])
            else:
                self.tail_pages[page_num+i].write(0)

        # Update page directory
        directory = [page_range_num, page_num, slot_num]
        self.page_directory[new_rid] = directory

        # Decrement tail rid
        self.tail_rid -= 1

        return True


    def sum(self, start_range, end_range, col_index_to_add):
        total = 0

        record_list = self.index.locate_range(start_range, end_range, column = 0)

        # No records found within range
        if len(record_list) == 0:
            return False

        for record in record_list:
            rid = record.rid

            #Grab page locations from page_directory
            pages = self.page_directory[rid] 

            indirection = self.base_pages[pages[PAGE_NUM_COL]+INDIRECTION_COLUMN].grab_slot(pages[SLOT_NUM_COL])

            # If updates exist
            if indirection != NULL_PTR:
                pages_tail = self.page_directory[indirection]

                schema = self.tail_pages[pages_tail[PAGE_NUM_COL]+SCHEMA_ENCODING_COLUMN].grab_slot(pages_tail[SLOT_NUM_COL])
                leadingZeros = "0" * self.num_columns
                schema_string = leadingZeros + str(schema)
                num = self.num_columns * -1
                schema_string = schema_string[num:]

                # If the column we want has been updated
                if schema_string[col_index_to_add] == "1":
                    val_to_add = self.tail_pages[pages_tail[PAGE_NUM_COL] + NUM_DEFAULT_COLUMNS + col_index_to_add].grab_slot(pages_tail[SLOT_NUM_COL])
                    total = total + val_to_add
                # Use values from base pages
                else:
                    total = total + self.base_pages[pages[PAGE_NUM_COL] + NUM_DEFAULT_COLUMNS + col_index_to_add].grab_slot(pages[SLOT_NUM_COL])
            # Use values from base pages
            else:
                total = total + self.base_pages[pages[PAGE_NUM_COL] + NUM_DEFAULT_COLUMNS + col_index_to_add].grab_slot(pages[SLOT_NUM_COL])

        return total


    def delete(self, key):
        try:
            record = self.index.locate(column = 0, value = key)[0]

            record.rid = None
            return True
        except:
            return False