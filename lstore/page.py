from lstore.config import *
import sys


class Page:

    def __init__(self, page_num):
        self.page_num = page_num
        self.num_records = 0
        self.data = bytearray(PAGE_CAPACITY_IN_BYTES)

    def has_capacity(self):
        # print("NUMRECORDS:", self.num_records, "slots:", PAGE_CAPACITY_IN_BYTES/INTEGER_CAPACITY_IN_BYTES)
        return self.num_records < int(PAGE_CAPACITY_IN_BYTES/INTEGER_CAPACITY_IN_BYTES)
        
    def next_empty_slot(self):
        slot_num = self.num_records * 8
        return slot_num

    def update_slot(self, rid, value): 
        val_as_bytes = value.to_bytes(INTEGER_CAPACITY_IN_BYTES, 'big')
        slot_start = (rid-1) % int(PAGE_CAPACITY_IN_BYTES/INTEGER_CAPACITY_IN_BYTES) 
        # print("updating RID:", rid, "with page_num:", self.page_num, "and slot_start:", slot_start, "value:", value)
        slot_num = slot_start * INTEGER_CAPACITY_IN_BYTES

        for byte_index in range(INTEGER_CAPACITY_IN_BYTES): 
            self.data[slot_num + byte_index] = val_as_bytes[byte_index]
        # self.num_records += 1
        

    def write_slot(self, rid, value): # needs to be tested
        val_as_bytes = value.to_bytes(INTEGER_CAPACITY_IN_BYTES, 'big')
        slot_start = (rid-1) % int(PAGE_CAPACITY_IN_BYTES/INTEGER_CAPACITY_IN_BYTES) 
        if DEBUG_MODE: print("RID:", rid, "page_num:", self.page_num, "slot_start:", slot_start)
        slot_num = slot_start * INTEGER_CAPACITY_IN_BYTES
        allocated = 0

        if self.has_capacity():
            for byte_index in range(INTEGER_CAPACITY_IN_BYTES): 
                try:
                    self.data[slot_num + byte_index] = val_as_bytes[byte_index]
                except:
                    if DEBUG_MODE: print("REWRITINGGGGGGGGGG---------------------- page:", self.page_num)
                    if allocated == 0:
                        self.data = bytearray(PAGE_CAPACITY_IN_BYTES)
                    allocated = 1
                    self.data[slot_num + byte_index] = val_as_bytes[byte_index]
            self.num_records += 1
        else:
            if DEBUG_MODE: print("OUTOFRANGE base")
            raise IndexError("Out of Range!")


    def write_slot_tail(self, rid, value): 
        rid = (rid - MAX_INT) - 1  #999 - 999 = 0 -1 = -1
        rid = rid * -1 # -1 * -1 = 1

        allocated = 0

        if(value == None):
            value = 0
            return
            
        val_as_bytes = value.to_bytes(INTEGER_CAPACITY_IN_BYTES, 'big')
        slot_start = (rid-1) % int(PAGE_CAPACITY_IN_BYTES/INTEGER_CAPACITY_IN_BYTES) 
        # print("RID:", rid, "page_num:", self.page_num, "slot_start:", slot_start)
        slot_num = slot_start * INTEGER_CAPACITY_IN_BYTES

        # if self.has_capacity():
        for byte_index in range(INTEGER_CAPACITY_IN_BYTES): 
            # print("Page_num",self.page_num,"slot_num",slot_num,"byte_index", byte_index)
            # print(self.data[slot_num + byte_index])
            try:
                self.data[slot_num + byte_index] = val_as_bytes[byte_index]
            except:
                if allocated == 0:
                    self.data = bytearray(PAGE_CAPACITY_IN_BYTES)
                allocated = 1
                self.data[slot_num + byte_index] = val_as_bytes[byte_index]
            self.num_records += 1
        # else:
        #     print("OUTOFRANGE tail")
        #     raise IndexError("Out of Range!")
        
      
    def write(self, value):
        # print("write: Entering Write!")
        val_as_bytes = value.to_bytes(INTEGER_CAPACITY_IN_BYTES, 'big')
        slot_num = self.num_records * INTEGER_CAPACITY_IN_BYTES
        # print("VALUE:",value)

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
        if DEBUG_MODE: print("Page ", self.page_num)
        slot_val = []
            
        for bytearray_index in range(len(self.data)):
            slot_val.append(self.data[bytearray_index])
            
            if len(slot_val) == INTEGER_CAPACITY_IN_BYTES:
                if DEBUG_MODE: print(slot_val, end=' = ')
                if DEBUG_MODE: print(self.broken_bytes_to_int(slot_val))
                slot_val.clear()


    def grab_slot(self, slot_num): #reading from memeory
        byte_val = []
        for byte_i in range(INTEGER_CAPACITY_IN_BYTES):
            try:
                byte_val.append(self.data[byte_i + (slot_num * INTEGER_CAPACITY_IN_BYTES)])
            except:
                if DEBUG_MODE: print("byte_val doesnt exist")

        integer = self.broken_bytes_to_int(byte_val)
        return integer
