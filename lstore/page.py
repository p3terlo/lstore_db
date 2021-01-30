from lstore.config import *

PAGE_CAPACITY_IN_BYTES = 32 
INTEGER_CAPACITY_IN_BYTES = 8

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_CAPACITY_IN_BYTES)


    def has_capacity(self):
        return self.num_records < int(PAGE_CAPACITY_IN_BYTES/INTEGER_CAPACITY_IN_BYTES)


    def is_full(self): #returns true if slots are filled
        return self.num_records == int(PAGE_CAPACITY_IN_BYTES/INTEGER_CAPACITY_IN_BYTES)
           

    def write(self, value):
        val_as_bytes = value.to_bytes(INTEGER_CAPACITY_IN_BYTES, 'big')
        slot_num = self.num_records * INTEGER_CAPACITY_IN_BYTES

        if self.has_capacity():
            for byte_index in range(INTEGER_CAPACITY_IN_BYTES):  
                self.data[slot_num + byte_index] = val_as_bytes[byte_index]
            self.num_records += 1
        else:
            raise IndexError("Out of Range!")


    @staticmethod
    def broken_bytes_to_int(value): 
        #conversion function for broken up bytes
        total = 0
        for byte in value:
            total = total * 256 + byte
        return total


    def display_internal_memory(self):

        print("\nPage")
        slot_val = []
            
        for bytearray_index in range(len(self.data)):
            slot_val.append(self.data[bytearray_index])
            
            if len(slot_val) == INTEGER_CAPACITY_IN_BYTES:
                print(slot_val, end=' = ')
                print(self.broken_bytes_to_int(slot_val))
                slot_val.clear()

                
    def get_page_capacity(self): 
        return PAGE_CAPACITY_IN_BYTES


    def grab_slot(self, slot_num): #reading from memeory
        byte_val = []
        for byte_i in range(INTEGER_CAPACITY_IN_BYTES):
            byte_val.append(self.data[byte_i + (slot_num * INTEGER_CAPACITY_IN_BYTES)])

        integer = self.broken_bytes_to_int(byte_val)
        return integer
