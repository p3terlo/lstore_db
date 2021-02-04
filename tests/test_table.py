import unittest

from lstore.table import Table
from lstore.config import PAGE_CAPACITY_IN_BYTES, INTEGER_CAPACITY_IN_BYTES, PAGE_RANGE

# PAGE_RANGE = 2000 records

class TestTable(unittest.TestCase):

    
    def setUp(self) -> None:
        self.table = Table(name="Grades", num_columns=5, key=0)

        print(f"Page Range: {PAGE_RANGE} Records")
        print(f"Page Capacity: {PAGE_CAPACITY_IN_BYTES} Bytes")
        print(f"Integer Capacity: {INTEGER_CAPACITY_IN_BYTES} Bytes")


    # def test_rid_to_slot_id_and_starting_page_id(self):
    #     """
    #     Depends on values in Config.py
    #     """
    #     rid = 10

    #     output = self.table.rid_to_slot_id_and_starting_page_id(num_columns=9, rid=rid)
    #     print(output["slot_id"])
    #     print(output["starting_page_id"])


    def test_add(self):
        self.table.add(*[92106420, 5, 12, 2, 10])
        # self.table.add(*[92106421, 6, 12, 2, 11])
        # self.table.add(*[92106422, 7, 12, 2, 12])
        # self.table.add(*[92106423, 8, 12, 2, 13])
        # self.table.add(*[92106424, 9, 12, 2, 14])

        self.table.update(92106420, *[None, None, None, None, 20])
        self.table.update(92106420, *[None, None, None, 19, 20])
        
        print("Printing base pages")
        for base_page in self.table.base_pages:
            base_page.display_internal_memory()

        print("Printing tail pages")
        for tail_page in self.table.tail_pages:
            tail_page.display_internal_memory()


    # def test_create_page_range(self):
    #     self.table.create_page_range()
    #     print(f"Num of base pages: {len(self.table.base_pages)}")
    #     print(f"Num of tail pages: {len(self.table.tail_pages)}")


    # def test__init__(self):
    #     pass