from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import *

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    # def __init__(self, transactions = []):
    #     self.stats = []
    #     self.transactions = transactions
    #     self.result = 0
    #     pass

    def __init__(self, transactions = None):
        self.stats = []
        self.transactions = []
        self.result = 0
        pass
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)
        THREAD_MASTER.append(t)

    """
    Runs a transaction
    """
    def run(self):
        for transaction in self.transactions:
            #push to transaction Queue for planning threads top\pull from


            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

    def display_worker(self):
        print("len(transactions)",len(self.transactions))
        for i in range(len(self.transactions)):
            self.transactions[i].print_queries()
            #print(self.transactions[i].display_transaction())
