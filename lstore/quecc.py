import queue
from threading import Thread
from lstore.config import NUM_QUEUES, NUM_THREADS
from lstore.transaction import Transaction


# Create a Thread using Class
# See https://thispointer.com/create-a-thread-using-class-in-python/
class PlanningThread(Thread):

    def __init__(self, priority_num, transactions):
        Thread.__init__(self)
        self.priority = priority_num
        self.transactions = transactions
        self.num_queues = NUM_QUEUES
        self.queues = []


    def init_queues(self):
        for _ in range(self.num_queues):
            q = queue.Queue()
            self.queues.append(q)
            

    # Takes a list of transactions as input
    def add_transactions(self):
        for transaction in self.transactions:
            for query, args in transaction.queries:
                key = args[0] % self.num_queues
                self.queues[key].put((query,args))


    # Override the run() function of Thread class
    def run(self):
        self.init_queues()
        self.add_transactions()
    

    # Only for testing purposes
    # Be careful because this pops off the queue, thus actually emptying the queue in the process
    def print_queue(self):
        i = 0
        for queue in self.queues:
            print(f"Queue #{i}")

            while not queue.empty():
                query = queue.get()
                print(query)

            i += 1


class PlanningThreadManager():

    def __init__(self, transactions):
        self.transactions = transactions
        self.num_threads = NUM_THREADS
        self.threads = []


    # Start up threads and group transactions into respective thread
    def init_threads(self):

        group = len(self.transactions) // self.num_threads

        for i in range(self.num_threads):
            start = group * i
            end = group * (i + 1)

            # In case of leftover transaction
            if (i == self.num_threads - 1 and end != len(self.transactions)):
                end = len(self.transactions)

            thread = PlanningThread(i, self.transactions[start:end])
            thread.start()
            self.threads.append(thread)

        for thread in self.threads:
            thread.join()


    def print_threads(self):
        for thread in self.threads:
            print(f"Thread #{thread.priority}")
            thread.print_queue()
            print("\n") 