from lstore.quecc import *

def main():
    transactions = []

    for i in range(20):
        transaction = Transaction()
        transaction.add_query('add', *[i, 'a'])
        transaction.add_query('delete', *[i+1, 'b'])

        transactions.append(transaction)

    manager = PlanningThreadManager(transactions)
    manager.init_threads()
    manager.print_threads()

main()