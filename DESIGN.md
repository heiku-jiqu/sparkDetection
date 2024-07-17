# Estimates

- VARCHAR(5000)
  - Char size = 1 to 4 byte per character
  - num char = 1 to 5000 chars
- BIGINT = uint64 = 8 byte
Approx event message size = ~5000bytes
10_000 events per second = 50mb per sec
10 * 60 * 60 * 24 = 894,000,000 events per day = 4.32 TB per day
326,310,000,000 events per year = 1631.55 TB per year

# Design
Distributed Message Broker for fault tolerance
7 day retention for that 1 day of realtime

Hashtable to distinct ~14,400,000 * 8bytes worth of data inmem
Assume is just duplicated data, not updates to fields, so only need to hash the key.

Stream Processing (flink?/pinot?) / Realtime Columnar DB (Clickhouse?/Materalize?/QuestDB?) for 1 Day of realtime updates
  - Considerations: Insert rate shd be quite fast, query shd see freshest data (low latency)
  - Need to filter out duplicate, no need watermark cause assume its not updates (immutable)
  - Need to decide on retention period for the realtime storage
Batch Processing for T-n Days of historical updates
  - What needs to be queried?
Merge tables using UNION?
  - May want a "semantic layer"/"query layer"/"virtualised layer" to present the UNION-ed table
    for users to use

When using Clickhouse, if its distributed, will need to read from written node to
minimise latency in the new events showing up in the query, 
due to its underlying eventual consistency of its distributed design.

How/where to store the data (real time and historical)?
For historical, will likely need to store in cloud object storage 

Other considerations:
- Frequency of events throughout the day may not be the same => likely have peak hours for vid detection.
  - System will need to handle the bandwidth of peak workloads
- High availability / Resilience / Uptime (eliminate single point of failure, have reliable crossover, detection of failures as they occur)
- Query patterns
  - What kind of queries (simple or complex)?
  - Simple queries maybe our execution engine no need to be so complex, simplifying the system
  - Complex queries see how to tune storage/indexes/preaggregation??
- Real size of item names, and # of unique item names 
  - Dictionary encoding will likely make the dataset much smaller ondisk and inmem
- "Realtime" latency requirements, is the realtime measured in milliseconds or under 10 seconds?
  - If latency is not so strict (<10 seconds), we can consider simplying the stack by using just Sparks' Structured Streaming
  - Reduce the need to maintain 1 more framework/system.
  - Spark Structured Streaming can help to do streaming deduplication and write to object store/table formats like delta

# Side note on going distributed
There is value in seriously considering the actual need for distributed systems.
Above assumptions take quite a worst case scenario in terms of data volume
(i.e. full fat 5000bytes unique item_name is sent for every event, with no compression at all).

But if we were to take a more modest estimate of each record being about 50 bytes
(4 BIGINTs + ~20 byte VARCHARs) and that there will likely be duplicated item names,
which is ~100x smaller than above estimation (without factoring compression yet). 
We will then arrive at a much more palatable data volume of
43.2 GB per day and 16 TB per year. 

According to AWS/Azure/GCP VM offerings in Southeast Asia, we are able to spin up
max 12 / 11 / 4 TB RAM instances to work with our data, so a single node solution
is very possible to handle the streaming daily workload.
  - No need to handle distributed system
  - Easier to setup
  - Easier to maintain
  - Easier to debug

[Scalability! But at what COST?](https://www.usenix.org/system/files/conference/hotos15/hotos15-paper-mcsherry.pdf)
