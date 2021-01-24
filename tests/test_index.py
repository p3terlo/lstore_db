import unittest 

from lstore.index import Index
from lstore.table import Table

class TestIndex(unittest.TestCase):

    table = index = None

    def setUp(self) -> None:
        
        self.table = Table("Grades", 5, key=0)
        self.index = Index(self.table)
        

    def test__init__(self):
        # One index for each table. All our empty initially.
        print(self.index.indices)
        pass

    """
    # returns the location of all records with the given value on column "column"
    """


    def test_locate(self):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """


    def test_locate_range(self):
        pass

    """
    # optional: Create index on specific column
    """


    def test_create_index(self):
        pass

    """
    # optional: Drop index of specific column
    """


    def test_drop_index(self):
        pass
