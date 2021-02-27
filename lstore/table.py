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
            print(f"Attepting to write to --> {self.name}: {total_columns}: {current_page}")
            frame = self.bufferpool.get_frame_from_pool(self.name, total_columns, current_page)
            frame.pin_page()
            print("Retrieved Frame!!")
            frame.page.display_internal_memory()
            frame.page.write_slot(rid, record_col[i])
            frame.make_dirty()
            frame.page.display_internal_memory()
            frame.unpin_page()
            print("Write Done!")
           

        # Update page directory
        directory = [page_range_num, starting_page_num, slot_num]
        self.page_directory[rid] = directory

        self.base_rid += 1

        
    def fetch_page(self, key):
        
        record = self.index.locate(column = 0, value = key)[0]
        rid = record.rid

        page_locations = self.page_directory[rid]
        page_id = page_locations[PAGE_NUM_COL]

        return page_id
