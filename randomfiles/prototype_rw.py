def write_page(self, key):

        #seek_offset = page_num/ num_columns (total)
        seek_offset = int(key/5)
        seek_mult = PAGE_CAPACITY_IN_BYTES
        
        col = key % 5 #num_columns

        bp = self.pages[key] #pages from table
        page = bp.page 

        
        #file_name = path/table_name + col_num.bin
        file_name = self.path + "/" + self.name + "_" + str(col) + ".bin"

        file= open(file_name, "wb") #binary

        file.seek(seek_offset * seek_mult) #seeking location for writing

        file.write(page.data) #writes entire bytearray onto file
       
        file.close()
        
def read_page(self, key):
        seek_offset = int(key/5)
        seek_mult = PAGE_CAPACITY_IN_BYTES
        col = key % 5 
        file_name = self.path + "/" + self.name + "_" + str(col) + ".bin"

        file = open(file_name, "rb")
        file.seek(seek_offset * seek_mult)

        
        data = file.read(8 * seek_mult) #read one bit at a time hence times 8
        
        #test = data[0:8]
        #test2 = data[8:16]
        #test3 = data[16:24]
        #test4= data[24:32]
        
        #print("test:", int.from_bytes(test, "big"))
        #print("test:", int.from_bytes(test2, "big"))
        #print("test:", int.from_bytes(test3, "big"))
        #print("test:", int.from_bytes(test4, "big"))
        
        file.close()
