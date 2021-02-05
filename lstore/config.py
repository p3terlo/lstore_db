# Global Setting for the Database
# PageSize, StartRID, etc..
import sys

PAGE_RANGE = 2000
PAGE_CAPACITY_IN_BYTES = 4096
INTEGER_CAPACITY_IN_BYTES = 8

# Macros for table.py
RID_COLUMN = 0
INDIRECTION_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
NUM_DEFAULT_COLUMNS = 4

NULL_PTR = 0

# Macros for page directory
PAGE_RANGE_COL = 0
PAGE_NUM_COL = 1
SLOT_NUM_COL = 2

MAX_INT = int(sys.maxsize)