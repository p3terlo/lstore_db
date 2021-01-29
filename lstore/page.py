from lstore.config import *

MEM_SIZE = 24

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(MEM_SIZE)

    def has_capacity(self):
        pass

    def is_full(self): #returns true if slots are filled
        if self.num_records == MEM_SIZE/8:
            return True
        return False

    def write(self, value):
        val_to_bytes = value.to_bytes(8, 'big') #converting 64bit int to bytes
        slot_num = self.num_records * 8

        
        for i in range(8): #adding 64 bits into 8 bytes 
            self.data[slot_num + i] = val_to_bytes[i]

        self.num_records += 1
        pass

    def byte_int_conver(self, byt): #conversion function for broken up bytes
        total = 0
        for i in byt:
            total = total * 256 + i
        #print(total)
        return total

    def display_mem(self): #displays internal memeory

        print("\nPage")
        slot_val =[]
            
        for i in range(len(self.data)):
            slot_val.append(self.data[i])
            
            if len(slot_val) == 8:
                print(slot_val, end=' = ')
                print(self.byte_int_conver(slot_val))
                slot_val.clear()
                
    def get_size(self): #returns the maximum_size of a page for calculations
        return MEM_SIZE

    def grab_slot(self, slot_num): #reading from memeory
        byte_val = []
        for i in range(8):
            byte_val.append(self.data[i + (slot_num * 8)])

        val = self.byte_int_conver(byte_val)
        return val