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
            q = []
            self.queues.append(q)
            

    # Takes a list of transactions as input
    def add_transactions(self):
        for transaction in self.transactions:
            for query, args in transaction.queries:
                key = args[0] % self.num_queues
                self.queues[key].append((query,args))


    # Override the run() function of Thread class
    def run(self):
        self.init_queues()
        self.add_transactions()
    

    def print_queue(self):
        i = 0
        for queue in self.queues:
            print(f"Queue #{i}")
            for query in queue:
                print(query)

            i += 1


class PlanningThreadManager():

    def __init__(self, transactions):
        self.transactions = transactions
        self.num_threads = NUM_THREADS
        self.threads = {}


    # Start up threads and passes subset of transactions into respective thread
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
            self.threads[i] = thread

        for thread in self.threads.values():
            thread.join()


    def print_threads(self):
        for priority, thread in self.threads.items():
            print(f"Thread #{priority}")
            thread.print_queue()
            print("\n")


class ExecutionThread(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        self.stats = []


    def execute_operations(self):
        if self.queue:
            for query, args in self.queue:
                result = query(*args)
                if result != False:
                    result = True
                self.stats.append(result)


    def run(self):
        self.execute_operations()


class ExecutionThreadManager():

    def __init__(self, planningThreadManager):
        self.planningThreadManager = planningThreadManager
        self.num_threads = NUM_THREADS if NUM_THREADS <= NUM_QUEUES else NUM_QUEUES # Ensures execution of higher priority queues before lower priority queues
        self.ordered_queues = []
        self.threads = []


    def init_threads(self):
        index = 0
        num_planningThreads = self.planningThreadManager.num_threads

        # Add queues in planning threads by priority to one list
        for i in range(num_planningThreads):
            planningThread = self.planningThreadManager.threads[i]
            self.ordered_queues.extend(planningThread.queues)

        # Assign a thread to each queue and upon completion, move on to next available queue
        while index < len(self.ordered_queues):

            for _ in range(self.num_threads):
                thread = ExecutionThread(self.ordered_queues[index])
                thread.start()
                self.threads.append(thread)
                index += 1

            # Wait for a thread to finish before creating a new one and moving on to the next queue
            for thread in self.threads:
                print("LERROYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY JENKINNNSSS")
                thread.join()
                self.threads.remove(thread)

            print("adsgagadsgagadsgagadsgagadsgagadsgagadsgagadsgagadsgagadsgagadsgagadsgagadsgagadsgagadsgagadsgagadsgagadsgagadsgagadsgag")