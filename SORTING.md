# Distributed Computing Tasks II (Question 2)

## Assumption

I am assuming that the question is asking about how data is moved to the correct node 
so that data can be joined (i.e. asking about join algorithms).

## Sort (Join) Algorithms

There are 3 main join algorithms in spark:
- Sort Merge
- Shuffle Hash Join
- Broadcast Hash Join

This explanation excludes cartesian and nested loop joins.

### Sort Merge Join

The default used is Sort Merge join, where the two datasets are partitioned
based on the join key, such that rows with the same join key land in the same partition (node).
The rows are also sorted based on the join keys, so we will end up with
rows with the same key sorted by join key within same partition. 
Once both datasets' rows are sorted within the same node, the merging of two keys can happen
by iterating through both rows and comparing whether the join keys are the same.
The important part is this sorting can spill to disk, which allows for larger than memory
keys to still work (so it is more stable).
It is also useful when the datasets have already been sorted, so the join is just linear time.


### Shuffle Hash Join

Shuffle hash join starts by partitioning the two datasets based on the join key,
just like sort merge join.
The difference is that instead of sorting the rows, each partition will be
creating a hash map (join key => row) based on the smaller dataset. 
The larger dataset can then iterate over the rows and find the appropriate join keys 
from the hashmap and perform the join.
This join strategy can be more performant, as creating hash map for the smaller table's rows
within the partition is faster than than sorting both.
However, this algorithm may be more unstable as the hash map needs to fit entirely
into the node's memory, which we might not be able to predict due to data skews.

### Broadcast Hash Join

This is the fastest join algorithm out of the 3. Instead of shuffling like Shuffle Hash Join,
the smaller table is distributed to all the nodes, and hash map of the key is created locally.
This also helps to prevent shuffling of the bigger table as the bigger table can use the local
hash map to perform the join.
The main limitation is that the smaller table/hash map will need to fit into memory, so this
cannot always be used depending on the size of the dataset.

## Implementing in Dataframe

By default, Sort Merge Join will be used (default value of spark.sql.join.preferSortMergeJoin is true).
If the smaller table is <10mb (default value of spark.sql.autoBroadcastJoinThreshold), then Broadcast Hash join will be used.
Dataset B only has 10,000 rows, so likely the query planner will use Broadcast Hash Join to perform the join.
