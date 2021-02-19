from template.config import *

NUM_INT_BYTES = 8
PAGE_CAP = 16

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_CAP)

    def max_cap(self):
        return int(PAGE_CAP/NUM_INT_BYTES)
    
    def has_capacity(self):
        return self.num_records < self.max_cap()

    def write(self, value):
        val_as_bytes = value.to_bytes(NUM_INT_BYTES, 'big')
        slot_num = self.num_records * NUM_INT_BYTES

        if self.has_capacity():
            for i in range(NUM_INT_BYTES):
                self.data[slot_num + i] = val_as_bytes[i]
            self.num_records += 1
        else:
            raise IndexError("Out of Range")
    
    @staticmethod
    def broken_bytes_to_int(value): 
        #conversion function for broken up bytes
        total = 0
        for byte in value:
            total = total * 256 + byte
        return total

    def grab_slot(self, slot_num): #reading from memeory
        byte_val = []
        for byte_i in range(NUM_INT_BYTES):
            byte_val.append(self.data[byte_i + (slot_num * NUM_INT_BYTES)])

        integer = self.broken_bytes_to_int(byte_val)
        return integer
  
    def display(self):
        print("\nPrinting PageData")

        slot_data = []
        
        for i in range(self.max_cap()):
            start = i * NUM_INT_BYTES
            end = start + NUM_INT_BYTES
            slot_data.append(self.data[start:end])

        
        for i in range(self.max_cap()):
            print(self.grab_slot(i), slot_data[i])
            
        print()
