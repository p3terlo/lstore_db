# from lstore.db import Database
# from lstore.query import Query
# #from lstore.config import init

# from random import choice, randint, sample, seed
# #init()

# db = Database()
# db.open('./ECS165')

# grades_table = db.create_table('Grades', 5, 0)
# query = Query(grades_table)

# # repopulate with random data
# records = {}
# seed(3562901)
# for i in range(0, 1000):
#     key = 92106429 + i
#     # records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
#     records[key] = [key, 1, 2, 3, 4]
#     query.insert(*records[key])

# keys = sorted(list(records.keys()))
# print("Insert finished")


# for key in keys:
#     record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
#     error = False
#     for i, column in enumerate(record.columns):
#         if column != records[key][i]:
#             error = True
#     if error:
#         print('select error on', key, ':', record, ', correct:', records[key])
#     else:
#         print('select on', key, ':', record)
# print("Select finished")


# for _ in range(10):
#     for key in keys:
#         updated_columns = [None, None, None, None, None]
#         for i in range(1, grades_table.num_columns):
#             value = randint(0, 20)
#             updated_columns[i] = value
#             original = records[key].copy()
#             records[key][i] = value
#             query.update(key, *updated_columns)
#             record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
#             error = False
#             for j, column in enumerate(record.columns):
#                 if column != records[key][j]:
#                     error = True
#             if error:
#                 print('update error on', original, 'and', updated_columns, ':', record.columns, ', correct:', records[key])
#             else:
#                 print('update on', original, 'and', updated_columns, ':', record)
#             updated_columns[i] = None
# print("Update finished")


# for key in keys:
#     # grades_table.merge(key)
#     query.merge(key)


# for key in keys:
#     record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
#     error = False
#     for i, column in enumerate(record.columns):
#         if column != records[key][i]:
#             error = True
#     if error:
#         print('select error on', key, ':', record.columns, ', correct:', records[key])
#     else:
#         print('select on', key, ':', record)
# print("Select finished 2")

# """

# for i in range(0, 100):
#     r = sorted(sample(range(0, len(keys)), 2))
#     column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
#     result = query.sum(keys[r[0]], keys[r[1]], 0)
#     if column_sum != result:
#         print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
#     # else:
#     #     print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
# print("Aggregate finished")
# """
# db.close()

from lstore.db import Database
from lstore.query import Query
# from lstore.config import init
import sys

from random import choice, randint, sample, seed
# init()

db = Database()
db.open('./ECS165')

grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

# repopulate with random data
records = {}
seed(3562901)
for i in range(0, 1000):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
keys = sorted(list(records.keys()))
print("Insert finished")

for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
        sys.exit(0)
    # else:
    #     print('select on', key, ':', record)
print("Select finished")


for _ in range(10):
    for key in keys:
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns):
            value = randint(0, 20)
            updated_columns[i] = value
            original = records[key].copy()
            records[key][i] = value
            query.update(key, *updated_columns)
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            error = False
            for j, column in enumerate(record.columns):
                if column != records[key][j]:
                    error = True
            if error:
                print('update error on', original, 'and', updated_columns, ':', record.columns, ', correct:', records[key])
                sys.exit(0)
            # else:
            #     print('update on', original, 'and', updated_columns, ':', record)
            updated_columns[i] = None
print("Update finished")
#     r = sorted(sample(range(0, len(keys)), 2))
#     column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
#     if column_sum != result:
#         print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
#     # else:
#     #     print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
# print("Aggregate finished")
db.close()
