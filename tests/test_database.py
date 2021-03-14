import unittest

from lstore.db import Database
from lstore.table import Table
from lstore.config import PAGE_CAPACITY_IN_BYTES, INTEGER_CAPACITY_IN_BYTES, PAGE_RANGE

# PAGE_RANGE = 2000 records

class TestDatabase(unittest.TestCase):

    
    def setUp(self) -> None:

        self.database = Database()
        self.database.open("./ECS165")
    


    def test_get_table(self):
        number_of_records_in_db = 1000
        table = self.database.get_table("Grades")
        print(table.index)

        table_rid = list(table.index.indices[0].values())

        # Assuming we are reading from 1000 records on disk.
        self.assertEqual(table_rid, [num for num in range(1, number_of_records_in_db)])

