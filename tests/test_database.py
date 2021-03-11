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
        table = self.database.get_table("Grades")
        print(table.index)
        