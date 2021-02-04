import unittest 
import BTrees

from lstore.index import Index
from lstore.table import Table


KEY_COLUMN = 0

DATA =  [
 [92108559, 17, 10, 15, 16],
 [92109735, 18, 19, 12, 5],
 [92106834, 9, 16, 11, 4],
 [92114154, 13, 3, 1, 9],
 [92109781, 9, 10, 10, 20],
 [92113505, 3, 7, 20, 1],
 [92109029, 18, 18, 4, 20],
 [92112962, 12, 10, 16, 12],
 [92111263, 1, 5, 14, 12],
 [92114080, 10, 8, 13, 2],
 [92106755, 14, 12, 11, 8],
 [92111147, 16, 3, 5, 10],
 [92114651, 20, 3, 12, 8],
 [92107807, 16, 2, 1, 5],
 [92112880, 13, 17, 16, 1],
 [92109854, 2, 20, 15, 12],
 [92107603, 9, 10, 1, 4],
 [92111296, 20, 20, 12, 19],
 [92107652, 11, 3, 4, 12],
 [92111717, 3, 17, 11, 19],
 [92114289, 16, 17, 13, 3],
 [92108477, 1, 14, 4, 0],
 [92115350, 18, 18, 10, 19],
 [92108350, 15, 6, 7, 17],
 [92107759, 10, 6, 14, 16],
 [92115221, 5, 0, 5, 6],
 [92113119, 0, 16, 20, 16],
 [92111203, 3, 4, 16, 20],
 [92114541, 12, 5, 14, 7],
 [92112364, 5, 6, 20, 9],
 [92113091, 19, 14, 19, 10],
 [92114951, 20, 15, 19, 3],
 [92109184, 0, 5, 15, 14],
 [92112984, 0, 18, 0, 1],
 [92107372, 5, 20, 6, 15],
 [92113322, 2, 3, 3, 15],
 [92112174, 14, 13, 14, 3],
 [92110441, 20, 3, 1, 4],
 [92109066, 20, 15, 16, 11],
 [92112462, 5, 5, 13, 20],
 [92111480, 17, 2, 14, 6],
 [92109423, 15, 19, 9, 19],
 [92107495, 19, 11, 6, 15],
 [92107795, 13, 7, 6, 19],
 [92113752, 10, 4, 15, 1],
 [92115147, 5, 10, 0, 20],
 [92109030, 8, 11, 11, 3],
 [92109498, 18, 7, 13, 6],
 [92111270, 6, 2, 2, 16],
 [92115073, 13, 8, 5, 13],
 [92115420, 4, 6, 13, 8],
 [92109259, 14, 5, 10, 0],
 [92107486, 12, 2, 1, 18],
 [92106565, 2, 7, 0, 1],
 [92111130, 1, 20, 10, 12],
 [92107992, 19, 7, 9, 10],
 [92107871, 12, 11, 12, 5],
 [92114705, 12, 8, 15, 8],
 [92113502, 17, 17, 13, 17],
 [92112673, 5, 19, 8, 7],
 [92113822, 20, 13, 19, 17],
 [92112556, 13, 1, 13, 7],
 [92109418, 15, 1, 16, 13],
 [92114070, 17, 19, 16, 18],
 [92112366, 1, 10, 3, 13],
 [92113873, 16, 10, 15, 11],
 [92112021, 15, 12, 15, 3],
 [92109749, 1, 3, 14, 16],
 [92113011, 18, 18, 12, 6],
 [92114237, 13, 7, 14, 6],
 [92108463, 9, 20, 14, 12],
 [92107626, 11, 7, 14, 4],
 [92112677, 11, 0, 5, 17],
 [92110273, 11, 13, 5, 0],
 [92109247, 12, 8, 1, 8],
 [92107465, 13, 5, 5, 2],
 [92108051, 7, 1, 8, 0],
 [92111514, 7, 2, 18, 11],
 [92113373, 9, 10, 2, 5],
 [92111956, 18, 14, 20, 1],
 [92111117, 0, 4, 11, 20],
 [92109391, 3, 12, 13, 11],
 [92114181, 0, 17, 7, 13],
 [92113419, 12, 4, 3, 6],
 [92114357, 9, 3, 7, 1]
]


class TestIndex(unittest.TestCase):

    table = index_for_table = None

    def setUp(self) -> None:
        
        self.data = DATA
        self.table = Table("Grades", 5, key=KEY_COLUMN)
        self.index_for_table = self.table.index
        rid = 0

        """Table Class Normally Handles the insertion into an index""" 
        for record in DATA:
            key = record[KEY_COLUMN]
            index_for_table = self.index_for_table.indices[KEY_COLUMN]
            index_for_table.insert(key, rid)
            rid += 1


    def test__init__(self):
        # One index_for_table for each table. All our empty initially.
        self.assertTrue(type(self.index_for_table.indices[KEY_COLUMN]), BTrees.IOBTree.IOBTree)
        
        number_of_records_inserted = DATA.__len__()
        number_of_records_in_key_column_index = self.index_for_table.indices[KEY_COLUMN].__len__()

        self.assertEqual(number_of_records_inserted, number_of_records_in_key_column_index)

    def test_locate(self):
        record_key = 92110441
        record_RID = [37]
        record_not_existing = 5045

        self.assertEqual(record_RID, self.index_for_table.locate(column=KEY_COLUMN, value=record_key))
        self.assertEqual(None, self.index_for_table.locate(column=KEY_COLUMN, value=record_not_existing))


    def test_locate_range(self):
        min_key = 92113322
        max_key = 92115022

        RID_LIST = [35, 78, 83, 58, 5, 44, 60, 65, 63, 9, 3, 82, 69, 20, 84, 28, 12, 57, 31]
        RID_LIST.sort()

        self.assertEqual(RID_LIST, self.index_for_table.locate_range(begin=min_key, end=max_key, column=KEY_COLUMN))


    def test_locate_range_query_window_out_of_range(self):        
        non_existent_low = 0
        non_existent_high = 5

        self.assertEqual([], self.index_for_table.locate_range(begin=non_existent_low, end=non_existent_high, column=KEY_COLUMN))

        
    @unittest.skip(" 'We are going to assume index_for_table is created at start of Table instantiation for Milestone 1' - TA")
    def test_create_index_for_table(self):
        pass


    def test_drop_index(self):
        self.index_for_table.drop_index(column_number=KEY_COLUMN)
        self.assertEqual(None, self.index_for_table.indices[KEY_COLUMN])
        

    def test_insert_multiple_values_for_single_key(self):
        key = 5
        value = 14
        value_2 = 20
        value_3 = 50

        self.index_for_table.insert(1, value)
        self.index_for_table.insert(1, value_2)
        self.index_for_table.insert(1, value_3)


    def test_locate_multiple_values_for_single_key(self):
        key = 5
        value = 14
        value_2 = 50
        value_3 = 20

        sorted_values = [14, 20, 50]

        self.index_for_table.insert(1, value)
        self.index_for_table.insert(1, value_2)
        self.index_for_table.insert(1, value_3)

        self.assertEqual(sorted_values, self.index_for_table.locate(column=0, value=1))


    def test_locate_range_multiple_values_for_single_key(self):
        key = 5
        value = 14
        value_2 = 20
        value_3 = 50

        sorted_values = [14, 20, 43, 50, 243, 443]

        self.index_for_table.insert(1, value)
        self.index_for_table.insert(1, value_2)
        self.index_for_table.insert(1, value_3)
        self.index_for_table.insert(2, 43)
        self.index_for_table.insert(3, 443)
        self.index_for_table.insert(5, 243)

        self.assertEqual(sorted_values, self.index_for_table.locate_range(begin=1, end=5, column=0))


    
    