
from BTrees.IOBTree import IOBTree 

class Index:
    
    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.create_index(column_number=0)


    def locate(self, column=0, value=None):
        if self.indices[column] is not None:
            located_value = self.indices[column].get(value)
            isNotList = not isinstance(located_value, list)

            if isNotList and located_value is not None:
                located_value = [located_value]
            elif located_value is None:
                located_value = [-1]

            return located_value    
        else:
            raise IndexError(f"No Index at Column {column}!")


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
        else:
            raise IndexError(f"No Index at Column {column}!")


    def create_index(self, column_number=0):
        self.indices[column_number] = IOBTree()
        # FIXME !!
        # Copy all the data in table to B-TREE
        # For Milestone 1, we are only creating_index in col 0
        # So I left as is.
        pass


    def drop_index(self, column_number=0):
        self.indices[column_number].clear()
        self.indices[column_number] = None


    def insert(self, key, value, column=0):
        if self.indices[column] is not None:
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
        else:
            raise IndexError(f"No Index at Column {column}!")

    
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
