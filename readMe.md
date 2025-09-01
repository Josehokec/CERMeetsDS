
# When Complex Event Recognition Meets Disaggregated Storage


Complex Event Recognition, abbreviated as CER

Disaggregated Storage, abbreviated as DS


## Insight
According to prior research work, when disaggregating compute and storage resources, network becomes bottleneck.
Thus, when processing CER queries, our key insight is to minimize the network overhead during compute node pulls events from storage nodes.
Low network overhead can be achieved by transmitting as few events as possible, i.e.,
identifying the shortest time intervals that contain matches and transmit only events within those intervals.

However, identifying the shortest time intervals is non-trivial, typically,
it would have a high communication cost or computational cost to obtain such time intervals.

To circumvent such issues, we propose a dual-filtering strategy that leverages both temporal and predicate constraints to incrementally shrink the intervals.
Besides, we propose shrinking window filter and window-wise bloom filter to assist identifying the shorter time intervals.


Note that we use [thrift](https://thrift.apache.org/) to build a distributed storage. The reasons why not use `Amazon S3` as storage are as follows:

- S3 only provides simple independent predicate granularity filtering using `S3 Select`
  (*e.g.,* select * from table where table.attribute < 10),
  this paper addresses additionally using the window condition and dependent condition to filter more data.

- We adopt multiple rounds of communication to filter out irrelevant events. However, **S3 is stateless**,
  so it cannot cache previous results.


## Running

* Step 1: Uniformly split the original dataset into $N$ small csv files (see [split_func2.py](src%2Fmain%2Fdataset%2Fsplit_func2.py)), where $N$ is the number of storage nodes
* Step 2: Convert these csv files to byte file in row format (see [ByteStore.java](src%2Fmain%2Fjava%2Fstore%2FByteStore.java))
* Step 3: Modify nodeId in FullScan (see [FullScan.java](src%2Fmain%2Fjava%2Fstore%2FFullScan.java))
* Step 4: Set and run storage service (see [StorageServer.java](src%2Fmain%2Fjava%2Frpc%2FStorageServer.java))
* Step 5: Set the IPs and ports for storage nodes, then run the specified approach (see [RunPushDown.java](src%2Fmain%2Fjava%2Fcompute%2FRunPushDown.java))

```java
String[] storageNodeIps = {"localhost", "your_ip_address"}; 
int[] ports = {9090, 9090};
```

## Program Description

### Package
| Name    | Explanation                                                                    |
|---------|--------------------------------------------------------------------------------|
| compute | Some approaches running on compute nodes                                       |
| engine  | SASE-E engine (optimized [SASE](https://github.com/haopeng/sase))              |
| event   | Event classes used by Flink and Esper, please note that Sase does not use them |
| filter  | Shrinking window filter and window-wise join filter                            |
| hasher  | Hash functions                                                                 |
| parser  | query parser for complex event query                                           |
| plan    | Variable processing order + Cost Model                                         |
| request | SQL queries that contain match_recognize keywords                              |
| rpc     | Remote procedure call service                                                  |
| store   | Store event into a byte file (row-format)                                      |
| utils   | Frequently used classes, e.g., ReplayIntervals                                 |

### Clarification

We want to highlight that we have elaborately optimized SASE rather than adopting it unchanged.
The implementation details can be found in package engine.
The core idea is to (1) reduce the copying of events through pointer references when generating matches; 
and (2) timely remove intermediate results that cannot form a final match.

We generate an event stream (see test.stream), where streamSize = 1000000, maxPrice = 100, numOfSymbol = 20, maxVolume = 1000, and randomSeed = 10.

The table below shows the query latency under four matching/evaluation engines (we run each query ten times).

| Engines              | Query Q1  | Query Q2  | Query Q3   |
| -------------------- | --------- | --------- | ---------- |
| Improved SASE (Ours) | 183.3ms   | 295.0ms   | 881.9ms    |
| [Original SASE](https://github.com/haopeng/sase)        | 14107.2ms | 18252.2ms | 59628.8ms  |
| [Esper](https://github.com/espertechinc/esper)                | 573.6ms   | 5949.2ms  | 52834.9ms  |
| Flink                | 2645.3ms  | 28759.7ms | 240477.5ms |


Queries are below:
```
# Query Q1
PATTERN SEQ(stock1 a, stock7 b, stock4 c)
WHERE skip-till-any-match
AND a.price < 30
AND b.price < a.price
AND c.price > 80
WITHIN 100

# Query Q2
PATTERN SEQ(stock1 a, stock7 b, stock4 c)
WHERE skip-till-any-match
AND a.price < 30
AND b.price < a.price
AND c.price > 80
WITHIN 1000

# Query Q3
PATTERN SEQ(stock1 a, stock7 b, stock4 c, stock3 d, stock2 e)
WHERE skip-till-any-match
AND a.price < 30
AND b.price < a.price
AND c.price > 80
AND d.price < 20
AND e.price < 10
WITHIN 1000
```
## Experimental Datasets

We use three real-world datasets, their details are as follow:

Please run our python code *e.g.*, [data_clean_crimes.py](src/main/dataset/data_clean_crimes.py) to clean the original datasets.

| Dataset name                                                                             | Event number | Size of single event |
|------------------------------------------------------------------------------------------|--------------|----------------------|
| [Crimes](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2/) | 8,115,536    | 76                   |
| [Citibike](https://citibikenyc.com/system-data)                                          | 34,994,042   | 57                   |
| [Cluster](https://github.com/google/cluster-data)                                        | 143,822,998  | 40                   |


We also provide [generate_synthetic_dataset.py](src/main/dataset/generate_synthetic_dataset.py) to produce synthetic datasets.

## Future work
- Support multiple thread for insertion, lookup, and filtering
- Accelerate CER in memory disaggregated environments
