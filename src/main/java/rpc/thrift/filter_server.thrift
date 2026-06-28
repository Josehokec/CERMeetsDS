
// please run below command to generate java code
// thrift -gen java filter_server.thrift

struct FilteredResult{
    1: i32 filteredNum;
    2: binary updatedSWF;
}

struct SameDataChunk{
    1: i32 chunkId;
    2: binary data;         // we always ensure data.length % recordSize = 0
    3: bool isLastChunk;
}

service FilterBasedRPC{
    // initialization
    map<string, i32> initial(1:string tableName, 2:map<string, list<string>> ipStrMap);

    // generate replay interval set that maybe contain matched results
    // input: variable name, query window, the head or tail marker for this variable
    // return: replay intervals
    binary getInitialIntervals(1:string varName, 2:i64 window, 3:i32 headTailMarker);

    // get bloom filter for join operation, here we need to send shrink filter to filter
    // input: variable name, query window, dependent predicates map, expected number of keys for each bloom filter, shrink filter buffer or updated markers
    // e.g., varName = t2, and dependent predicates are t1.a = t2.a and t1.b = t2.b
    // then dpStrMap: <t1, [t1.a = t2.a, t1.b = t2.b]>
    // if current variable is the second variable to access, then buff is shrink filter
    // otherwise, buffer is updated markers
    // return: bloom filters
    map<string, binary> getBF4EQJoin(1:string varName, 2:i64 window, 3:map<string, list<string>> dpStrMap, 4:map<string, i32> keyNumMap, 5:binary buff)

    // input: variable name, query window, dependent predicates map, the head or tail marker for this variable, shrink filter buffer or updated markers
    // if current variable is the second variable to access, then buff is shrink filter
    // otherwise, buffer is updated markers
    FilteredResult windowFilter(1:string varName, 2:i64 window, 3:i32 headTailMarker, 4:binary buff);

    // if current variable and previous variables have equal dependent predicates, then we call this function
    // input: variable name, query window, the head or tail marker for this variable, sequence map, dependent predicates map, bool filters map
    FilteredResult eqJoinFilter(1:string varName, 2:i64 window, 3:i32 headTailMarker, 4:map<string, bool> previousOrNext, 5:map<string, list<string>> dpStrMap, 6:map<string, binary> bfBufferMap)

    // send filtered events
    SameDataChunk getAllFilteredEvents(1:i64 window, 2:binary updatedSWF);
}
