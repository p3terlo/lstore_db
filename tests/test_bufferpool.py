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


    def test_read_page_from_disk(self):
        page_number = 4
        number_columns = 8
        table_name = "Grades"
        self.grades_table.bufferpool.assign_path("tests/files")
        read_page = self.grades_table.bufferpool.read_page_from_disk(table_name, page_number, number_columns)
        self.assertIs(type(read_page), Page)


    @unittest.SkipTest
    def test_add_page(self):
        pass


    # @unittest.SkipTest
    def test_write_slot(self):
        RID = 3
        new_value = 69696969
        page_number = 4
        number_columns = 8
        table_name = "Grades"
        self.grades_table.bufferpool.assign_path("tests/files")
        read_page = self.grades_table.bufferpool.read_page_from_disk(table_name, page_number, number_columns)
        read_page.display_internal_memory()
        read_page.write_slot(RID, new_value)
        read_page.display_internal_memory()


    def test_write_slot_empty(self):
        RID = 3
        page = Page(1)
        new_value = 69696969
        page.write_slot(RID, new_value)
        page.display_internal_memory()



 