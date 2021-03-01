from lstore.bufferpool import Frame
from lstore.config import *
from lstore.index import Index
from lstore.page import *
from lstore.bufferpool import BufferPool

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
        self.bufferpool = ""

        # Page directory should map RID to [page range, page, offset (AKA slot number)]
        self.page_directory = {}
        self.index = Index(self)

        # Added structures
        self.base_pages = []
        self.tail_pages = []

        self.base_rid = 1
        self.tail_rid = MAX_INT

    def pass_bufferpool(self, bufferpool):
        self.bufferpool = bufferpool

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
        '''
        Maps a given RID -> corresponding slot_id within a page according to the 
        PAGE_CAPACITY_IN_BYTES and INTEGER_CAPACITY_IN_BYTES constraints.

        Additionally gives the beginning page_id which a record belongs to depending on
        the num_columns given.
        '''
        """        
        999-999 =  0-1 = -1 * -1 = 1
        998-999 = -1-1 = -2 * -1 = 2
        """
        print("calculate rid",rid)
        rid = (rid - MAX_INT) -  1
        rid = rid * -1

        # We subtract 1 from RID because first RID starts at 1
        output = {}

        slots_per_page = int(PAGE_CAPACITY_IN_BYTES / INTEGER_CAPACITY_IN_BYTES)
        output[SLOT_NUM_COL] = (rid - 1) % slots_per_page

        page_offset = int((rid - 1) / slots_per_page) 
        output[PAGE_NUM_COL] = (num_columns) * page_offset

        page_range_num = int((rid - 1) / PAGE_RANGE)
        output[PAGE_RANGE_COL] = page_range_num

        return output
    


    def create_base_record(self, *columns):
        # Create record
        rid = self.base_rid
        record_key = columns[self.key]
        milliseconds = int(round(time.time() * 1000))
        schema = 0

        record_col = [rid, NULL_PTR, milliseconds, schema]
        
        for column in columns:
            record_col.append(column)

        return Record(rid, record_key, record_col)

    def create_tail_record(self, *columns, previous_rid, schema):
        rid = self.tail_rid
        record_key = columns[self.key]
        milliseconds = int(round(time.time() * 1000))

        record_col = [rid, previous_rid, milliseconds, schema]
        
        for column in columns:
            record_col.append(column)

        return Record(rid, record_key, record_col)


    # def sample_read(self):

    #     bufferpool = self.bufferpool

    #     for i in range(8):
    #         bufferpool.read_page(self.name, i, self.num_columns + NUM_DEFAULT_COLUMNS)
            
        
    #     print("done")

    def add(self, *columns):

        # Initialize values
        rid = self.base_rid
        record_key = columns[self.key]

        base_record = self.create_base_record(*columns)
        record_col = base_record.columns

        total_columns = len(record_col)

        print("\ntotal columns to write:", record_col)
        
        # Insert record in index
        # Key -> record -> RID
        self.index.insert(record_key, base_record) #update

        # Get page number from RID
        page_dict = self.calculate_base_page_numbers(total_columns, rid)

        slot_num = page_dict[SLOT_NUM_COL]
        starting_page_num = page_dict[PAGE_NUM_COL]
        page_range_num = page_dict[PAGE_RANGE_COL]

        # print(f"Starting page num: {starting_page_num}")

        # Write record to pages
        for i in range(total_columns):

            current_page = starting_page_num + i
            ###print(f"Attepting to write to --> {self.name}: {total_columns}: {current_page}")
            frame = self.bufferpool.get_frame_from_pool(self.name, total_columns, current_page)
            frame.pin_page()
            print("Retrieved Frame!")
            frame.page.display_internal_memory()
            frame.page.write_slot(rid, record_col[i])
            print("writing", record_col[i])
            frame.make_dirty()
            frame.page.display_internal_memory()
            frame.unpin_page()
            print("WRITE DONE")
           
        # Update page directory
        directory = [page_range_num, starting_page_num, slot_num]
        print("inserting", rid, "into page directory")
        self.page_directory[rid] = directory

        self.base_rid += 1

    #FIXME Check for updates
    def select(self, key, column, query_columns):

        record_col = []
        record_array = []

        record = self.index.locate(column=0, value=key)[0]
        rid = record.rid

        page_dict = self.page_directory[rid]
        starting_page_num = page_dict[PAGE_NUM_COL]
        slot_num = page_dict[SLOT_NUM_COL]
        indirection_page_num = starting_page_num + INDIRECTION_COLUMN
        total_columns = self.num_columns + NUM_DEFAULT_COLUMNS
        base_indirection_frame = self.bufferpool.get_frame_from_pool(self.name, total_columns, indirection_page_num)

        for i in range(NUM_DEFAULT_COLUMNS, NUM_DEFAULT_COLUMNS + self.num_columns):
            # if query_columns[i - NUM_DEFAULT_COLUMNS] == 1:
            frame = self.bufferpool.get_frame_from_pool(self.name, 9, starting_page_num+i)
            value = frame.page.grab_slot(slot_num)
            # print(value)
            record_col.append(value)

        indirection_value = base_indirection_frame.page.grab_slot(slot_num)

        if indirection_value != NULL_PTR:
            print("PREVIOUS UPDATES: INDIRECTION =", indirection_value)
            tail_page_dict = self.calculate_tail_page_numbers(total_columns, indirection_value)
            tail_page_num = tail_page_dict[PAGE_NUM_COL]
            tail_slot_num = tail_page_dict[SLOT_NUM_COL]
            tail_page_frame = self.bufferpool.get_frame_from_pool_tail(self.name, total_columns, tail_page_num + SCHEMA_ENCODING_COLUMN)
            schema_int = tail_page_frame.page.grab_slot(tail_slot_num)
            print("SCHEMA_INT", schema_int, "tail_page_num",tail_page_num, "tail_slot_num", tail_slot_num, "total_columns",total_columns)
             # Convert from int to useable string format EX: 10 -> "00010"
            leadingZeros = "0" * self.num_columns
            schema_string = leadingZeros + str(schema_int)
            num = self.num_columns * -1
            schema_string = schema_string[num:]

            print("SCHEMA_STRING",schema_string)
            for i in range(len(schema_string)):
                if schema_string[i] == "1": 
                    print("FOUND A CHANGE") 
                    tail_page_to_add_frame = self.bufferpool.get_frame_from_pool_tail(self.name, total_columns, tail_page_num + NUM_DEFAULT_COLUMNS+i)
                    tail_page_to_add_frame.pin_page()
                    print("defaults + i", NUM_DEFAULT_COLUMNS + i, "slot:", tail_slot_num)
                    record_col[NUM_DEFAULT_COLUMNS + i] = tail_page_to_add_frame.page.grab_slot(tail_slot_num)
                    tail_page_to_add_frame.unpin_page()



        print(record_col)
        record_to_return = Record(rid,key,record_col)  
        record_array.append(record_to_return)

        print("****************End of Select********************")

        return record_array

    def update(self, key, *columns):
        #Get base record and base rid
        print("Beginning Update")
        record = self.index.locate(column=0, value=key)[0]
        
        #RECORD NOT FOUND FIXME CHECK IF RECORD DELETED
        if record == -1:
            return False

        base_rid = record.rid

        #Get page locations of base record
        page_dict = self.page_directory[base_rid]
        starting_page_num = page_dict[PAGE_NUM_COL]
        slot_num = page_dict[SLOT_NUM_COL]
        base_page_indirection_num = starting_page_num + INDIRECTION_COLUMN
        print("starting page:", starting_page_num, "slot_num:", slot_num,"base_page_indirection_num:",base_page_indirection_num)

        #Handle schema: 00010 -> 10
        schema = ""
        for column in columns:
            if column == None:
                schema += "0"
            else:
                schema += "1"
        schema = int(schema)
        print("schema", schema)

        #Create tail record and grab columns
        tail_record_to_add = self.create_tail_record(*columns, previous_rid=base_rid, schema=schema)
        tail_record_rid = tail_record_to_add.rid
        record_col = tail_record_to_add.columns
        total_columns = len(record_col)
        print("total_columns", total_columns,"record_col", record_col)

        #Calculate tail page numbers
        tail_page_dict = self.calculate_tail_page_numbers(total_columns, tail_record_rid)
        tail_page_num = tail_page_dict[PAGE_NUM_COL]
        tail_slot_num = tail_page_dict[SLOT_NUM_COL]
        tail_page_range_num = tail_page_dict[PAGE_RANGE_COL]
        print("Tail_page_num",tail_page_num, "Tail_slot_num",tail_slot_num)

        #Get base record indirection page
        base_indirection_frame = self.bufferpool.get_frame_from_pool(self.name, total_columns, base_page_indirection_num)
        indirection_value = base_indirection_frame.page.grab_slot(slot_num)

        if indirection_value != 0:
            print("Previously updated")
            #Set old update as indirection for new update
            record_col[INDIRECTION_COLUMN] = indirection_value

            #Grab locations of old update
            print("indirection:", indirection_value)
            old_update_page_dict = self.calculate_tail_page_numbers(total_columns, indirection_value)

            #Grab Schema of old update
            old_update_starting_page = old_update_page_dict[PAGE_NUM_COL]
            old_update_schema_page = old_update_starting_page + SCHEMA_ENCODING_COLUMN

            print("old_update_starting_page", old_update_starting_page)

            old_schema_frame = self.bufferpool.get_frame_from_pool_tail(self.name, total_columns, old_update_schema_page)
            old_schema_int = old_schema_frame.page.grab_slot(old_update_page_dict[SLOT_NUM_COL])
            
            #grab current schema
            # current_schema_int = record_col[SCHEMA_ENCODING_COLUMN]
            current_schema_int = schema

            # Convert from int to useable string format EX: 10 -> "00010"
            leadingZeros = "0" * self.num_columns
            current_schema_string = leadingZeros + str(current_schema_int)
            num = self.num_columns * -1
            current_schema_string = current_schema_string[num:]
            old_schema_string = leadingZeros + str(old_schema_int)
            old_schema_string = old_schema_string[num:]

            new_schema_string = ""

            #Combine prev and current schema EX: 010 + 100 = 110
            for i in range(self.num_columns):
                if current_schema_string[i] == "1" or old_schema_string[i] == "1":
                    new_schema_string += "1"
                else: 
                    new_schema_string += "0"

            # Grab updates from previous updates, don't grab new updates or will overwrite them
            for i in range(len(old_schema_string)):
                if old_schema_string[i] == "1" and current_schema_string[i] != "1":
                    old_tail_frame = self.bufferpool.get_frame_from_pool_tail(self.name,total_columns, old_update_starting_page + NUM_DEFAULT_COLUMNS + i)
                    print("successfully grabbed old_tail_frame")
                    record_col[NUM_DEFAULT_COLUMNS + i] = old_tail_frame.page.grab_slot(old_update_page_dict[SLOT_NUM_COL])

            print("UPDATING SCHEMA", new_schema_string, int(new_schema_string))
            record_col[SCHEMA_ENCODING_COLUMN] = int(new_schema_string)
        else:
            print("NO PREV UPDATING SCHEMA:", schema)
            record_col[SCHEMA_ENCODING_COLUMN] = schema


        for i in range(total_columns):
            current_page = tail_page_num + i

            print("Updating current_page:",current_page)
            frame = self.bufferpool.get_frame_from_pool_tail(self.name, total_columns,current_page)
            frame.pin_page()
            # frame.page.display_internal_memory()
            frame.is_tail = True
            frame.page.write_slot_tail(tail_record_rid, record_col[i])
            print("Writing", record_col[i])
            frame.make_dirty()
            frame.page.display_internal_memory()
            frame.unpin_page()

        #Write new tail record to base indirection
        print("replacing indirection")
        base_indirection_frame = self.bufferpool.get_frame_from_pool(self.name, total_columns, base_page_indirection_num)

        base_indirection_frame.pin_page()
        # base_indirection_frame.page.display_internal_memory()
        base_indirection_frame.page.update_slot(base_rid, tail_record_rid)
        base_indirection_frame.make_dirty()
        base_indirection_frame.page.display_internal_memory()
        base_indirection_frame.unpin_page()

        """UPDATE PAGE_DIRECTORY"""
        directory = [tail_page_range_num, tail_page_num, tail_slot_num]
        self.page_directory[tail_record_rid] = directory

        self.tail_rid -= 1
        # frame = self.bufferpool.get_frame_from_pool_tail(self.name, total_columns,tail_page_num+SCHEMA_ENCODING_COLUMN)
        # print(frame.page.grab_slot(tail_slot_num))
        print("END OF UPDATE>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        pass


    def fetch_page(self, key):
        
        record = self.index.locate(column = 0, value = key)[0]
        rid = record.rid

        page_locations = self.page_directory[rid]
        page_id = page_locations[PAGE_NUM_COL]

        return page_id
