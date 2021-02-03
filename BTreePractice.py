from BTrees.IIBTree import IIBTree
from BTrees.IOBTree import IOBTree 
from random import choice, randint, sample, seed

record_index = IOBTree()
rid_index = IIBTree()


# Insertion with Int -> Int mapping
for i in range(0, 1000):
    key = 92106429 + randint(0, 9000)
    RID = randint(0, 100000)
    rid_index.insert(key, RID)


# Insertion with Int -> Object mapping
for i in range(0, 1000):
    key = 92106429 + randint(0, 9000)
    record_index.insert(key, [RID, "Second Column", "Third Column", "Fourth Column", "Fifth Column"])



# subset = index.items(50, 100)


# Updating 
min_key = record_index.minKey(-4)
print(f"{min_key} Before Update: ", record_index[min_key])

record_index.update({min_key: [69, "New Second Column", "New Third Column", "New Fourth Column", "New Fifth Column"]})

print(f"{min_key} After Update: ", record_index[min_key])


# Deletion
print(f"{min_key} Before Delete: ", record_index[min_key])
record_index.pop(min_key)

if not record_index.has_key(min_key):
    print("Item Deleted!")
