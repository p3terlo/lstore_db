from random import choice, randint, sample, seed
import unittest 

from lstore.db import Database
from lstore.query import Query
from lstore.bufferpool import Frame, BufferPool

class TestBufferPool(unittest.TestCase):

# create DB, create table
# add 5 records or so

    

    def setUp(self):

        self.database = Database()
        self.grades_table = self.database.create_table('Grades', 5, 0)
        self.grades_table.pass_bufferpool(self.database.bufferpool)
    
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

        # for i in range(0, 100):
        #     bp = Frame(i, "hehexd", "table")
        #     self.database.bufferpool.add(bp)

        # self.database.bufferpool.print_pool()




 