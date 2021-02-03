import unittest 

from lstore.page import Page, INTEGER_CAPACITY_IN_BYTES, PAGE_CAPACITY_IN_BYTES
from lstore.table import Record

DATA = [25, 23, 65, 3, 43, 34, 86, 45, 23]


class TestPage(unittest.TestCase):

    def setUp(self) -> None:
        self.page = Page()

        for index, number in enumerate(DATA):
            
            if index < PAGE_CAPACITY_IN_BYTES / INTEGER_CAPACITY_IN_BYTES:
                self.page.write(number)

        self.page.display_internal_memory()


    def test_write(self):

        for index, number in enumerate(DATA):

            byte_index = index * INTEGER_CAPACITY_IN_BYTES + 7
            
            if byte_index < PAGE_CAPACITY_IN_BYTES:
                self.assertEqual(number, self.page.data[byte_index])
        

    def test_broken_bytes_to_int(self):

        large_number = 1532     
        large_number_represented_as_byte_array = large_number.to_bytes(INTEGER_CAPACITY_IN_BYTES, "big")
        converted_back_to_integer = self.page.broken_bytes_to_int(large_number_represented_as_byte_array)

        self.assertEqual(large_number, converted_back_to_integer)
        

    def test_grab_slot(self):

        page_data = []
        
        for data_index in range(len(self.page.data)):   
            byte_index = data_index * INTEGER_CAPACITY_IN_BYTES + 7

            if byte_index < PAGE_CAPACITY_IN_BYTES:
                number = self.page.grab_slot(data_index)
                page_data.append(number)

        # We choose subset because to handle situation where page size is very small
        subset_of_data = DATA[:len(page_data)]

        self.assertEqual(subset_of_data, page_data)
        
        



