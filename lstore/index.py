from BTrees.IOBTree import IOBTree 
"""
A data structure holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:
    
    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.create_index(column_number=0)


    def __str__(self):
        output = "\nTable index item count: \n"
        for i, index in enumerate(self.indices):
            if type(index) is IOBTree:
                output += f"Column {i}: {len(index)}\n"
            else:
                output += f"Column {i}: No Index Initialized.\n"
        return output


    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column=0, value=None):
        if self.indices[column] is not None:
            located_value = self.indices[column].get(value)
            isNotList = not isinstance(located_value, list)

            if isNotList and located_value is not None:
                located_value = [located_value]
            
        if located_value == None:
            return [-1]

        return located_value
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column=0):
        if self.indices[column] is not None:
            located_values = list(self.indices[column].values(begin, end))
            values_to_delist = []
            
            for index, value in enumerate(located_values):
                if isinstance(value, list):
                    for val in value:
                        values_to_delist.append(val)
            
            located_values = [value for value in located_values if not isinstance(value, list)]
            located_values = located_values + values_to_delist
            # located_values.sort()
            
            return located_values

    """
    # optional: Create index on specific column
    """

    # def create_index(self, column_number=0):
    #     self.indices[column_number] = IOBTree()

    def create_index(self, column_number = 0):
        if column_number < 0 or column_number >= len(self.indices):
            raise  IndexError(f"No Column {column_number} in Table.")

        elif self.indices[column_number] is None:
            self.indices[column_number] = IOBTree()

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number=0):
        self.indices[column_number].clear()
        self.indices[column_number] = None


    def insert(self, key, value, column=0):

        valueNeedsToBeList = self.indices[column].has_key(key)

        if valueNeedsToBeList:
            popped_value = self.indices[column].pop(key)
            valueAlreadyList = isinstance(popped_value, list)

            if valueAlreadyList:
                value_list = [*popped_value, value]
            else:
                value_list = [popped_value, value]

            value_list.sort()
            
            self.indices[column].insert(key, value_list)
        else: 
            self.indices[column].insert(key, value)

    def update(self, key, old_value, new_value, column=0):
        if self.indices[column] is not None:
            if self.indices[column].has_key(key):
                values = self.indices[column].pop(key)

                if len(values) == 1:
                    self.indices[column].insert(key, new_value)
                else:
                    new_values = [value for value in values if value != old_value] + [new_value]
                    self.indices[column].insert(key, new_values)
        else:
            raise IndexError(f"No Index at Column {column}!")

