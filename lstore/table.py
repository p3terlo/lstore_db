from .bufferpool import Frame
from .config import *
from .index import Index
from .page import *
from .bufferpool import BufferPool

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
        # print("calculating tail page numbers for rid:",rid)
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


    @staticmethod
    def schema_int_to_string(schema, num_columns):
        leadingZeros = "0" * num_columns
        schema_string = leadingZeros + str(schema)
        num = num_columns * -1
        schema_string = schema_string[num:]
        
        return schema_string

    @staticmethod
    def combine_schemas(current_schema_string, old_schema_string, num_columns):
        new_schema_string = ""
        #Combine prev and current schema EX: 010 + 100 = 110
        for i in range(num_columns):
            if current_schema_string[i] == "1" or old_schema_string[i] == "1":
                new_schema_string += "1"
            else: 
                new_schema_string += "0"

        return new_schema_string

    @staticmethod
    def schema_array_to_schema_int(*columns):
        schema = ""
        for column in columns:
            if column == None:
                schema += "0"
            else:
                schema += "1"
        schema = int(schema)
        
        return schema
    


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


    def get_frame(self, page_num):#Alvin
        return self.bufferpool.fetch_frame(self.name, self.num_columns + NUM_DEFAULT_COLUMNS, page_num)


    def get_frame_tail(self, page_num):#Alvin
        print("get_frame called", page_num)
        return self.bufferpool.fetch_frame_tail(self.name, self.num_columns + NUM_DEFAULT_COLUMNS, page_num)


    def add2(self, *columns): #Alvin
        base_record = self.create_base_record(*columns)
        record_col = base_record.columns
        total_columns = len(record_col)

        page_dict = self.calculate_base_page_numbers(total_columns, self.base_rid)
        
        starting_page_num = page_dict[PAGE_NUM_COL]

        for i in range(total_columns):
            current_page = starting_page_num + i
            frame = self.get_frame(current_page)
            frame.write_slot(self.base_rid, record_col[i])

        #index/page_directory
        self.index.insert(columns[self.key], self.base_rid)
        directory = [page_dict[PAGE_RANGE_COL], starting_page_num, page_dict[SLOT_NUM_COL]]
        self.page_directory[self.base_rid] = directory
        self.base_rid += 1
        

    def select(self, key, column, query_columns):
        print("Selecting key:", key)
        record_col = []
        record_array = []

        rid = self.index.locate(column=0, value=key)[0]

        if rid == None:
            return [False]

        page_dict = self.page_directory[rid]
        starting_page_num = page_dict[PAGE_NUM_COL]
        slot_num = page_dict[SLOT_NUM_COL]
        indirection_page_num = starting_page_num + INDIRECTION_COLUMN
        total_columns = self.num_columns + NUM_DEFAULT_COLUMNS
        base_indirection_frame = self.get_frame(indirection_page_num)

        #GETS ALL VALUES
        for i in range(NUM_DEFAULT_COLUMNS, NUM_DEFAULT_COLUMNS + self.num_columns):
            # if query_columns[i - NUM_DEFAULT_COLUMNS] == 1:
            frame = self.get_frame(starting_page_num+i)
            value = frame.page.grab_slot(slot_num)
            record_col.append(value)


        # for i in range(NUM_DEFAULT_COLUMNS, NUM_DEFAULT_COLUMNS + self.num_columns):
        #     if query_columns[i - NUM_DEFAULT_COLUMNS] == 1:
        #         frame = self.get_frame(starting_page_num+i)
        #         value = frame.page.grab_slot(slot_num)
        #         record_col.append(value)

        indirection_value = base_indirection_frame.page.grab_slot(slot_num)

        if indirection_value != NULL_PTR:
            print("PREVIOUS UPDATES: INDIRECTION =", indirection_value)
            tail_page_dict = self.calculate_tail_page_numbers(total_columns, indirection_value)
            tail_page_num = tail_page_dict[PAGE_NUM_COL]
            tail_slot_num = tail_page_dict[SLOT_NUM_COL]
            tail_page_frame = self.get_frame_tail(tail_page_num + SCHEMA_ENCODING_COLUMN)
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
                    tail_page_to_add_frame = self.get_frame_tail(tail_page_num + NUM_DEFAULT_COLUMNS+i)
                    # tail_page_to_add_frame.pin_page()
                    print("tail_page_num", tail_page_num, "tail_slot_num:", tail_slot_num, "i", i)
                    print(record_col)
                    record_col[i] = tail_page_to_add_frame.page.grab_slot(tail_slot_num)
                    # tail_page_to_add_frame.unpin_page()

        print(record_col)
        record_to_return = []

        for i in range(len(record_col)):
            if query_columns[i] == 1:
                record_to_return.append(record_col[i])

        record_col = record_to_return

        record_to_return = Record(rid,key,record_col)  
        record_array.append(record_to_return)
        print("End of select on key:", key, "\n")

        return record_array

    def update(self, key, *columns):
        print("\nBeginning Update on key:", key)
        print("will be applying:",columns)
        #Get base record and base rid
        base_rid = self.index.locate(column=0, value=key)[0]

        #RECORD NOT FOUND FIXME CHECK IF RECORD DELETED
        if base_rid == -1:
            return False
        # base_rid = record.rid

        #Get page locations of base record
        page_dict = self.page_directory[base_rid]
        starting_page_num = page_dict[PAGE_NUM_COL]
        slot_num = page_dict[SLOT_NUM_COL]
        base_page_indirection_num = starting_page_num + INDIRECTION_COLUMN
        # print("starting page:", starting_page_num, "slot_num:", slot_num,"base_page_indirection_num:",base_page_indirection_num)

        #Handle schema: 00010 -> 10
        schema = self.schema_array_to_schema_int(*columns)

        #Create tail record and grab columns
        tail_record_to_add = self.create_tail_record(*columns, previous_rid=base_rid, schema=schema)
        tail_record_rid = tail_record_to_add.rid
        record_col = tail_record_to_add.columns
        total_columns = len(record_col)
        # print("total_columns", total_columns,"record_col", record_col)

        #Calculate tail page numbers
        tail_page_dict = self.calculate_tail_page_numbers(total_columns, tail_record_rid)
        tail_page_num = tail_page_dict[PAGE_NUM_COL]
        tail_slot_num = tail_page_dict[SLOT_NUM_COL]
        tail_page_range_num = tail_page_dict[PAGE_RANGE_COL]
        # print("Tail_page_num",tail_page_num, "Tail_slot_num",tail_slot_num)

        #Get base record indirection page
        print("get_frame_indirection", key)
        base_indirection_frame = self.get_frame(base_page_indirection_num)
        indirection_value = base_indirection_frame.page.grab_slot(slot_num)
        print("Grabbing indirection:", indirection_value, "from page:", base_page_indirection_num)

        if indirection_value != NULL_PTR:
            print("Previously updated")
            #Grab Schema of old update
            record_col[INDIRECTION_COLUMN] = indirection_value
            print("indirection:", indirection_value)
            old_update_page_dict = self.calculate_tail_page_numbers(total_columns, indirection_value)
            old_update_starting_page = old_update_page_dict[PAGE_NUM_COL]
            old_update_schema_page = old_update_starting_page + SCHEMA_ENCODING_COLUMN
            # print("old_update_starting_page", old_update_starting_page)
            print("get_frame_old_schema" ,key)
            old_schema_frame = self.get_frame_tail(old_update_schema_page)
            old_schema_int = old_schema_frame.page.grab_slot(old_update_page_dict[SLOT_NUM_COL])

            #Grab current schema
            current_schema_string = self.schema_int_to_string(schema, self.num_columns)
            old_schema_string = self.schema_int_to_string(old_schema_int, self.num_columns)
            new_schema_string = self.combine_schemas(current_schema_string, old_schema_string, self.num_columns)

            # Grab updates from previous updates, don't grab new updates or will overwrite them
            for i in range(len(old_schema_string)):
                if old_schema_string[i] == "1" and current_schema_string[i] != "1":
                    print("get_frame_old_tail" ,key)
                    old_tail_frame = self.get_frame_tail(old_update_starting_page + NUM_DEFAULT_COLUMNS + i)
                    record_col[NUM_DEFAULT_COLUMNS + i] = old_tail_frame.page.grab_slot(old_update_page_dict[SLOT_NUM_COL])

            print("New schema string:", new_schema_string, "New schema int:",int(new_schema_string))
            record_col[SCHEMA_ENCODING_COLUMN] = int(new_schema_string)
        else:
            print("NO PREV UPDATING SCHEMA:", schema)
            record_col[SCHEMA_ENCODING_COLUMN] = schema

        print(record_col)

        for i in range(total_columns):
            print(i)
            current_page = tail_page_num + i
            # print("Updating current_page:",current_page)
            print("get_frame_tail_current",key)
            frame = self.get_frame_tail(current_page)
            frame.pin_page()
            # frame.page.display_internal_memory()
            frame.is_tail = True
            print("tail_record_rid", tail_record_rid, "record_col[i]", record_col[i])
            frame.page.write_slot_tail(tail_record_rid, record_col[i])
            # print("Writing", record_col[i])
            frame.make_dirty()
            # frame.page.display_internal_memory()
            frame.unpin_page()

        #Write new tail record to base indirection
        print("get_frame_indirection_frame", key)
        base_indirection_frame = self.get_frame(base_page_indirection_num)
        base_indirection_frame.pin_page()
        # base_indirection_frame.page.display_internal_memory()
        print("updating indirection w/",tail_record_rid, "which is on page:", base_page_indirection_num)
        base_indirection_frame.page.update_slot(base_rid, tail_record_rid)
        base_indirection_frame.make_dirty()
        # base_indirection_frame.page.display_internal_memory()
        base_indirection_frame.unpin_page()

        """UPDATE PAGE_DIRECTORY"""
        directory = [tail_page_range_num, tail_page_num, tail_slot_num]
        self.page_directory[tail_record_rid] = directory

        self.tail_rid -= 1
        print("End of update on key:", key,"\n")

    
    def merge(self, key):

        #Get RID
        base_rid = self.index.locate(column=0, value=key)[0]

        if base_rid == -1:
            return False

        #Get page locations of base record
        page_dict = self.page_directory[base_rid]
        base_page_num = page_dict[PAGE_NUM_COL]
        base_slot_num = page_dict[SLOT_NUM_COL]
        base_first_column_num = base_page_num + NUM_DEFAULT_COLUMNS
        base_page_indirection_num = base_page_num + INDIRECTION_COLUMN

        #Get indirection frame & value
        base_indirection_frame = self.get_frame(base_page_indirection_num)
        
        indirection_value = base_indirection_frame.page.grab_slot(base_slot_num)

        #Previously updated
        if indirection_value != NULL_PTR:
            #Grab newest tail record
            total_columns = self.num_columns + NUM_DEFAULT_COLUMNS
            tail_page_dict = self.calculate_tail_page_numbers(total_columns, indirection_value)
            tail_page_num = tail_page_dict[PAGE_NUM_COL]
            tail_slot_num = tail_page_dict[SLOT_NUM_COL]
            tail_first_column_num = tail_page_num + NUM_DEFAULT_COLUMNS
            tail_schema_num = tail_page_num + SCHEMA_ENCODING_COLUMN

            tail_schema_frame = self.get_frame_tail(tail_schema_num)
            tail_schema_int = tail_schema_frame.page.grab_slot(tail_slot_num)

            tail_schema_string = self.schema_int_to_string(tail_schema_int, self.num_columns)

            for i in range(self.num_columns):
                if tail_schema_string[i] == "1":

                    updated_value_frame = self.get_frame_tail(tail_first_column_num + i)
                    # updated_value_frame.pin_page()
                    updated_value = updated_value_frame.page.grab_slot(tail_slot_num)
                    # updated_value_frame.unpin_page()

                    base_page_frame = self.get_frame(base_first_column_num + i)
                    base_page_frame.pin_page()
                    base_page_frame.page.update_slot(base_rid, updated_value)
                    base_page_frame.make_dirty()
                    base_page_frame.unpin_page()

            print("changing to new indirection")

            base_indirection_frame = self.get_frame(base_page_indirection_num)
            base_indirection_frame.pin_page()
            base_indirection_frame.page.update_slot(base_rid, NULL_PTR)
            base_indirection_frame.make_dirty()
            base_indirection_frame.unpin_page()

            return

        else:
            return

# copy_of_base_pages = []
        # for _ in range(NUM_DEFAULT_COLUMNS + self.num_columns):
        #     new_base_page = Page()
        #     copy_of_base_pages.append(new_base_page)



    def delete(self, key):
        try:
            rid = self.index.locate(column=0, value=key)[0]
            rid = None

            return True
        except:
            return False







# def sample_read(self):

    #     bufferpool = self.bufferpool

    #     for i in range(8):
    #         bufferpool.read_page(self.name, i, self.num_columns + NUM_DEFAULT_COLUMNS)
            
        
    #     print("done")


# def fetch_page(self, key):
#         record = self.index.locate(column = 0, value = key)[0]
#         rid = record.rid
#         page_locations = self.page_directory[rid]
#         page_id = page_locations[PAGE_NUM_COL]

#         return page_id


 # def add(self, *columns):

    #     # Initialize values
    #     rid = self.base_rid
    #     record_key = columns[self.key]

    #     base_record = self.create_base_record(*columns)
    #     record_col = base_record.columns

    #     total_columns = len(record_col)

    #     print("\ntotal columns to write:", record_col)
        
    #     # Insert record in index
    #     # Key -> record -> RID
    #     self.index.insert(record_key, base_record) #update

    #     # Get page number from RID
    #     page_dict = self.calculate_base_page_numbers(total_columns, rid)

    #     slot_num = page_dict[SLOT_NUM_COL]
    #     starting_page_num = page_dict[PAGE_NUM_COL]
    #     page_range_num = page_dict[PAGE_RANGE_COL]

    #     # print(f"Starting page num: {starting_page_num}")

    #     # Write record to pages
    #     for i in range(total_columns):

    #         current_page = starting_page_num + i
    #         ###print(f"Attepting to write to --> {self.name}: {total_columns}: {current_page}")
    #         frame = self.bufferpool.get_frame_from_pool(self.name, total_columns, current_page)
    #         frame.pin_page()
    #         print("Retrieved Frame!")
    #         frame.page.display_internal_memory()
    #         frame.page.write_slot(rid, record_col[i])
    #         print("writing", record_col[i])
    #         frame.make_dirty()
    #         frame.page.display_internal_memory()
    #         frame.unpin_page()
    #         print("WRITE DONE")
           
    #     # Update page directory
    #     directory = [page_range_num, starting_page_num, slot_num]
    #     print("inserting", rid, "into page directory")
    #     self.page_directory[rid] = directory

    #     self.base_rid += 1


    # def sum(self, start_range, end_range, col_index_to_add):
    #     total = 0

    #     record_list = self.index.locate_range(start_range, end_range, column = 0)

    #     # No records found within range
    #     if len(record_list) == 0:
    #         return False

    #     for record in record_list:
    #         rid = record.rid

    #         #Grab page locations from page_directory
    #         pages = self.page_directory[rid] 
    #         page_num = pages[PAGE_NUM_COL]
    #         slot_num = pages[SLOT_NUM_COL]

    #         base_indirection_frame = self.bufferpool.get_frame_from_pool(self.name, NUM_DEFAULT_COLUMNS + self.num_columns, indirection_page_num)


    #         indirection = self.base_pages[pages[PAGE_NUM_COL]+INDIRECTION_COLUMN].grab_slot(pages[SLOT_NUM_COL])

    #         # If updates exist
    #         if indirection != NULL_PTR:
    #             pages_tail = self.page_directory[indirection]

    #             schema = self.tail_pages[pages_tail[PAGE_NUM_COL]+SCHEMA_ENCODING_COLUMN].grab_slot(pages_tail[SLOT_NUM_COL])
    #             leadingZeros = "0" * self.num_columns
    #             schema_string = leadingZeros + str(schema)
    #             num = self.num_columns * -1
    #             schema_string = schema_string[num:]

    #             # If the column we want has been updated
    #             if schema_string[col_index_to_add] == "1":
    #                 val_to_add = self.tail_pages[pages_tail[PAGE_NUM_COL] + NUM_DEFAULT_COLUMNS + col_index_to_add].grab_slot(pages_tail[SLOT_NUM_COL])
    #                 total = total + val_to_add
    #             # Use values from base pages
    #             else:
    #                 total = total + self.base_pages[pages[PAGE_NUM_COL] + NUM_DEFAULT_COLUMNS + col_index_to_add].grab_slot(pages[SLOT_NUM_COL])
    #         # Use values from base pages
    #         else:
    #             total = total + self.base_pages[pages[PAGE_NUM_COL] + NUM_DEFAULT_COLUMNS + col_index_to_add].grab_slot(pages[SLOT_NUM_COL])

    #     return total