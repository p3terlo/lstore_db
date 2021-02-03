from BTrees.IOBTree import IOBTree 
"""
A data structure holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    
    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.create_index(column_number=0)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column=0, value=None):
        if self.indices[column] is not None:
            return self.indices[column].get(value)


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column=0):
        if self.indices[column] is not None:
            return list(self.indices[column].values(begin, end))

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number=0):
        self.indices[column_number] = IOBTree()
        # FIXME !!
        # Copy all the data in table to B-TREE
        # For Milestone 1, we are only creating_index in col 0
        # So I left as is.
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number=0):
        self.indices[column_number].clear()
        self.indices[column_number] = None
    
