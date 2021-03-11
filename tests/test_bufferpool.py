from random import choice, randint, sample, seed
import unittest 

from lstore.config import INTEGER_CAPACITY_IN_BYTES
from lstore.db import Database
from lstore.query import Query
from lstore.page import Page
from lstore.bufferpool import Frame, BufferPool



class TestBufferPool(unittest.TestCase):

# create DB, create table
# add 5 records or so

    

    def setUp(self):

        self.database = Database()
        self.database.open("./ECS165")
        self.grades_table = self.database.create_table('Grades', 5, 0)
        self.grades_table.pass_bufferpool(self.database.bufferpool)
    

    @unittest.SkipTest
    def test_add(self):
        self.query = Query(self.grades_table)
        records = {}

        seed(3562901)

        for i in range(0, 3):
            key = 92106429 + randint(0, 9000)
            while key in records:
                key = 92106429 + randint(0, 9000)
            records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
            self.query.insert(*records[key])
            print('inserted', records[key])

        self.database.bufferpool.print_pool()



    # @unittest.SkipTest
    def test_read_frame(self):
        number_columns = 8
        page_number = 0 + number_columns * 8
        
        table_name = "Grades"
        self.grades_table.bufferpool.assign_path("tests/files")
        frame = self.grades_table.bufferpool.read_frame(table_name, number_columns, page_number)
        frame.page.display_internal_memory()
        self.assertIs(type(frame), Frame)


    @unittest.SkipTest
    def test_read_frame_fail(self):

        wrong_page_number = -69696969
        number_columns = 8
        table_name = "Grades"
        self.grades_table.bufferpool.assign_path("tests/files")
        frame = self.grades_table.bufferpool.read_frame(table_name, number_columns, wrong_page_number)
        self.assertIs(type(frame), type(None))
        

    @unittest.SkipTest
    def test_add_page(self):
        pass


    @unittest.SkipTest
    def test_write_slot(self):
        RID = 3
        new_value = 69696969
        page_number = 4
        number_columns = 8
        table_name = "Grades"
        self.grades_table.bufferpool.assign_path("tests/files")
        read_frame = self.grades_table.bufferpool.read_frame(table_name, number_columns, page_number)
        print("Printing Page before write: \n")
        read_frame.page.display_internal_memory()
        read_frame.page.write_slot(RID, new_value)
        print("\nPrinting Page After write: \n")
        read_frame.page.display_internal_memory()


    @unittest.SkipTest
    def test_write_slot_empty(self):
        RID = 3
        page = Page(1)
        new_value = 69696969
        page.write_slot(RID, new_value)
        page.display_internal_memory()


    @unittest.SkipTest
    def test_create_new_page(self):
        number_columns = 8
        table_name = "Grades"
        read_frame = self.grades_table.bufferpool.create_new_page(table_name, number_columns)
        self.assertIs(type(read_frame), Frame)

 

 