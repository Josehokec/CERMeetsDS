
# When Complex Event Recognition Meets Cloud-Native Architectures

## Background

Complex Event Recognition (CER) aims to detect a predefined pattern composed of multiple primitive events.


Cloud-native architectures aim to decouple and pool computing and storage resources, enabling independent scaling. 
Since it offers elasticity, availability, and cost efficiency, many database vendors are migrating their products to cloud-native architectures.
We highlight that cloud-native technology is a major trend in data storage and querying.


When CER is implemented on cloud-native architectures, the network often becomes a performance bottleneck. 
Thus, in this paper, we address the following question: 
**how can we design an efficient pulling strategy to retrieve events from multiple storage nodes, so that compute nodes can minimize query latency for CER?**

## Insight
When processing CER queries, our key insight is to minimize the network overhead during compute node pulls events from storage nodes.
Low network overhead can be achieved by transmitting as few events as possible, i.e.,
identifying the shortest time intervals that contain matches and transmit only events within those intervals.

However, identifying the shortest time intervals is non-trivial, typically,
it would have a high communication cost or computational cost to obtain such time intervals.

To circumvent such issues, we propose a dual-filtering strategy that leverages both temporal and predicate constraints to incrementally shrink the intervals.
Besides, we propose shrinking window filter and window-wise bloom filter to assist identifying the shorter time intervals.

Note that we use [thrift](https://thrift.apache.org/) to implement node communication.


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
| Name    | Explanation                                                                  |
|---------|------------------------------------------------------------------------------|
| compute | Some approaches running on compute nodes                                     |
| engine  | SASE-E engine (optimized [SASE](https://github.com/haopeng/sase))            |
| event   | Event classes used by Flink and Esper, please note that Sase does not use them |
| filter  | Shrinking window filter and window-wise join filter                          |
| hasher  | Hash functions                                                               |
| parser  | query parser for complex event query                                         |
| plan    | Variable processing order + Cost Model                                       |
| request | SQL queries that contain match_recognize keywords                            |
| rpc     | Remote procedure call service                                                |
| store   | Store event into a byte file (row-format)                                    |
| utils   | Frequently used classes, e.g., ReplayIntervals                               |

```sql
-- query example (FlinkSQL supports 'within' keywords)
-- since N1 and N2 are not contributing to the query results (i.e., matches)
-- we can use a pushdown strategy to pull events that hold independent conditions
SELECT * FROM CITIBIKE MATCH_RECOGNIZE(
    ORDER BY eventTime
    MEASURES A.ride_id as AID, B.ride_id as BID, C.ride_id AS CID
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '5' MINUTE
    DEFINE
        A AS A.type = 'B' 
            AND A.start_lat >= 40.75123 
            AND A.start_lat <= 40.774903,
        B AS B.type = 'H' 
            AND B.start_station_id = A.end_station_id 
            AND B.start_lat >= 40.75123 
            AND B.start_lat <= 40.774903,
        C AS C.type = 'K' 
            AND C.start_station_id = B.end_station_id 
            AND C.start_lat >= 40.75123 
            AND C.start_lat <= 40.774903 
            AND C.start_lat <= B.start_lat 
            AND C.start_lng <= B.start_lng
);
```

### Performance comparison: SASE-E vs. SASE

We have elaborately optimized SASE rather than adopting it unchanged.
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
