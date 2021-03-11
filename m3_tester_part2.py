from lstore.db import Database
from lstore.quecc import *
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
from lstore.config import *

import sys
from random import choice, randint, sample, seed

# init()
db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)

keys = []
records = {}
seed(3562901)
num_threads = 8

try:
    grades_table.index.create_index(1)
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

transaction_workers = []
insert_transactions = []
select_transactions = []
update_transactions = []
for i in range(num_threads):
    # insert_transactions.append(Transaction())
    select_transactions.append(Transaction())
    update_transactions.append(Transaction())
    transaction_workers.append(TransactionWorker())
    # transaction_workers[i].add_transaction(insert_transactions[i])
    transaction_workers[i].add_transaction(select_transactions[i])
    transaction_workers[i].add_transaction(update_transactions[i])
worker_keys = [ {} for t in transaction_workers ]



# for i in range(0, 1000):
for i in range(0, 3):
    key = 92106429 + i
    keys.append(key)
    # i = i % num_threads
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    q = Query(grades_table)
    # print("adding to worker", i, *records[key])
    # insert_transactions[i].add_query(q.insert, *records[key])
    q.insert(*records[key])
    worker_keys[i][key] = True

t = 0
_records = [records[key] for key in keys]
# for c in range(grades_table.num_columns):
for c in range(1):

    _keys = sorted(list(set([record[c] for record in _records])))
    index = {v: [record for record in _records if record[c] == v] for v in _keys}
    for key in _keys:
        found = True
        for record in index[key]:
            if record[0] not in worker_keys[t % num_threads]:
                found = False
        if found:
            query = Query(grades_table)
            select_transactions[t % num_threads].add_query(query.select, key, c, [1, 1, 1, 1, 1])
        t += 1

for j in range(0, num_threads):
    for key in worker_keys[j]:
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns):
            value = randint(0, 20)
            updated_columns[i] = value
            records[key][i] = value
            query = Query(grades_table)
            update_transactions[j].add_query(query.update, key, *updated_columns)
            updated_columns = [None, None, None, None, None]


count = 0
# print(len(transaction_workers))
# for i in range(len(transaction_workers)):
#     print("Transaction Worker:", count)
#     #print(transaction_workers[i].display_worker())
#     transaction_workers[i].display_worker()
#     count += 1
    
# print("LIST")
# for item in THREAD_MASTER:
#     item.print_queries()

manager = PlanningThreadManager(THREAD_MASTER)
manager.init_threads()
manager.print_threads()

execution_manager = ExecutionThreadManager(manager)
execution_manager.init_threads()
    

# print("running")
# for transaction_worker in transaction_workers:
#      transaction_worker.run()

score = len(keys)
for key in keys:
     correct = records[key]
     query = Query(grades_table)
    
     result = query.select(key, 0, [1, 1, 1, 1, 1])[0].columns
     if correct != result:
         print('select error on primary key', key, ':', result, ', correct:', correct)
         score -= 1
print('Score', score, '/', len(keys))

db.close()

