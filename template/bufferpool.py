from template.page import *

class leastUsed:
    def __init__(self):
        self.queue = []
        self.length = 0

    def add(self, name, pos): #assuming mantains count of 2, (name, pos)
        val = (name, pos)
        if self.queue.count(val) != 1:
            self.queue.append(val)
            self.length += 1
        else:
            self.queue.remove(val)
            self.queue.append(val)
        
    def grabLU(self):

        if (self.length == 0):
            print("Error EMPTY")
            return (-1, -1)
        
        LU = self.queue.pop(0)
        self.length -= 1

        key = LU[0]
        pos = LU[1]

        #updating keys
        for i in range(len(self.queue)):
            val = self.queue[i]

            if val[0] == key:
                if val[1] > pos:
                    self.queue[i] = (key, val[1] - 1)

        return LU
    
    def display(self):
        print(self.queue)
        
    
class bufferPage:
    def __init__(self):
        self.rid_start = 0
        self.rid_end = 0

        #rid, data
        
        self.num_columns = 0
        self.col_pages = []
        self.isDirty = False

        
    def init_write(self, data):
        rid = data[0] #current assumption rid is 0
        self.rid_start = rid
        self.rid_end = rid + int(PAGE_CAP/NUM_INT_BYTES) - 1

        self.isDirty = True
        self.num_columns = len(data)

        for val in data:
            new_page = Page()
            new_page.write(val)
            self.col_pages.append(new_page)
        
    def write(self, data):

        for i in range(self.num_columns):
            self.col_pages[i].write(data[i])
        
        
            
    def in_range(self, rid):
        start = self.rid_start
        end = self.rid_end

        if rid >= start and rid <= end:
            return True
        else:
            return False


    def output2file(self, file):
        max_slot = self.col_pages[0].max_cap()

        for i in range(max_slot):
            
            record_string = ""
            for page in self.col_pages:
                record_string += str(page.grab_slot(i)) + " "
            #print(record_data)
            record_string += "\n"
            file.write(record_string)
        

    def display_page(self):
        for page in self.col_pages:
            page.display()
            
    def display_record(self, rid):
        #print("grabbing Record")
        max_slot = self.col_pages[0].max_cap()
        slot_num = (rid-1) % max_slot #-1 to compensate indexing

        record = []

        for page in self.col_pages:
            record.append(page.grab_slot(slot_num))
        print(record)
        

    def display_cur_records(self):
        max_slot = self.col_pages[0].max_cap()

        for i in range(max_slot):
            #print(i)
            self.display_record(i + 1)

        print()


        
class Bufferpool:
    def __init__(self):
        self.pool_cap = 10
        self.pool ={}
        self.db_path = ""
        self.lu = leastUsed()
        self.init = True
        
    def cur_cap(self):
        val = 0
        
        if len(self.pool) != 0:
            for key in self.pool:
                val += len(self.pool[key])
        #print(val)
        return val
    
    
    def init_poolkey(self, table_name):
        self.pool[table_name] = []
        #self.cur_cap()       

    def assign_path(self, path):
        self.db_path = path

    def find_page(self, name, rid):
        
        for page in self.pool[name]:
            #page.display_page()
            if (page.in_range(rid)):
                #print("match")
                return page

        return -1


    def display(self, name):
        print("curpath: ", self.db_path)
        print(self.pool)
        for page in self.pool[name]:
            page.display_cur_records()
    
    def add(self, name, data):
        #will deal with full later
        rid = data[0]
    
        #initializing pool
        if self.cur_cap() == 0: 
            fresh_page = bufferPage()
            fresh_page.init_write(data)
            self.pool[name].append(fresh_page)
            #this is last used
            self.lu.add(name, 0)
            
        else:
            pool_page = self.find_page(name, rid)

            if (pool_page == -1):
                #assuming new page (concurrent)
                
                if (self.cur_cap() != self.pool_cap): #if cap is not full
                    fresh_page = bufferPage()
                    fresh_page.init_write(data)
                    self.pool[name].append(fresh_page)
                    self.lu.add(name, len(self.pool[name]) - 1)
                    print("newPage",data)
                else: #eviction
                    print("evicting: ")
                    #self.lu.display()
                    evict_cords = self.lu.grabLU()
                    evict_name = evict_cords[0]
                    evict_pos = evict_cords[1]

                    print(evict_name, evict_pos)

                    if (self.init):
                        #print(evict_name, evict_pos)
                        evict_page = self.pool[evict_name][evict_pos]
                        self.pool[evict_name].remove(evict_page)
                        #self.init = False


                        self.flush(evict_name, evict_page)
                        
                        #need to replace
                        fresh_page = bufferPage()
                        fresh_page.init_write(data)
                        self.pool[name].append(fresh_page)
                        self.lu.add(name, len(self.pool[name]) - 1)
                        
                    
            else:
                pool_page_loc = self.pool[name].index(pool_page)
                pool_page.write(data)
                self.lu.add(name, pool_page_loc)
               
           
        print(data)
        
        pass

    def flush(self, table_name,page): #eviction to write
        mode = ""
        if (page.rid_start == 1):
            mode = "w"
        else:
            mode = "a"

        filename = self.db_path + "/" + table_name + ".txt"

        file = open(filename, mode)

        page.output2file(file)
        file.close()

    def db_close(self):
        for i in range(self.cur_cap()):#iterating remianing pool
            evict_cords = self.lu.grabLU()
            evict_name = evict_cords[0]
            evict_pos = evict_cords[1]
            
            evict_page = self.pool[evict_name][evict_pos]
            self.pool[evict_name].remove(evict_page)
            
            self.flush(evict_name, evict_page)#flush method
        print(self.pool)
