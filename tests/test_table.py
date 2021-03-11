import unittest

from lstore.db import Database
from lstore.table import Table
from lstore.config import PAGE_CAPACITY_IN_BYTES, INTEGER_CAPACITY_IN_BYTES, PAGE_RANGE

# PAGE_RANGE = 2000 records

class TestTable(unittest.TestCase):

    
    def setUp(self) -> None:

        self.database = Database()
        self.database.open("./ECS165")
        self.table = self.database.create_table('Grades', 5, 0)
        self.table.pass_bufferpool(self.database.bufferpool)
    
        print(f"Page Range: {PAGE_RANGE} Records")
        print(f"Page Capacity: {PAGE_CAPACITY_IN_BYTES} Bytes")
        print(f"Integer Capacity: {INTEGER_CAPACITY_IN_BYTES} Bytes")


    @unittest.SkipTest
    def test_rid_to_slot_id_and_starting_page_id(self):
        """
        Depends on values in Config.py
        """
        rid = 10

        output = self.table.rid_to_slot_id_and_starting_page_id(num_columns=9, rid=rid)
        print(output["slot_id"])
        print(output["starting_page_id"])


    @unittest.SkipTest
    def test_select(self):
        self.table.add2(*[92106420, 5, 12, 2, 10])
        self.table.update(92106420, *[None, None, None, None, 20])
        self.table.update(92106420, *[None, None, None, 19, 20])

        
        select = self.table.select(92106420, 0, [1, 0, 0, 1, 1])
        print(select)


    @unittest.SkipTest
    def test_add2(self):
        self.table.add2(*[92106420, 5, 12, 2, 10])
        self.table.add2(*[92106421, 6, 12, 2, 11])
        self.table.add2(*[92106422, 7, 12, 2, 12])
        self.table.add2(*[92106423, 8, 12, 2, 13])
        self.table.add2(*[92106424, 9, 12, 2, 14])

        self.table.update(92106420, *[None, None, None, None, 20])
        self.table.update(92106420, *[None, None, None, 19, 20])
        
