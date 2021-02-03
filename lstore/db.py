from template.table import Table

class Database():

    def __init__(self):
        self.tables = {}

    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        if name in self.tables:
            print("create_table Error: Table with name %s already exists" % (name))
        else:
            self.tables[name] = table
            return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        try:
            self.tables.pop(name)
        except KeyError:
            print("drop_table Error: No table exists with name %s" % (name))

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        try:
            table = self.tables[name]
            return table
        except KeyError:
            print("get_table Error: No table exists with name %s" % (name))
            
