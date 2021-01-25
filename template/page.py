from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(8)

    def has_capacity(self):
        pass

    def write(self, value):
        index = self.num_records
        self.data[index] = value
        self.num_records += 1
        pass

    def display(self):
        print(self.data)
        for i in range(self.num_records):
            print(self.data[i], end = ", ")
        print()
        print()
