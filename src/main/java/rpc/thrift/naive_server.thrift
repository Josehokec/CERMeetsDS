// please run below command to generate java code
// thrift -gen java naive_server.thrift

struct SDataChunk{
    1: i32 chunkId;
    2: binary data;         // we always ensure data.length % recordSize = 0
    3: bool isLastChunk;
}

service BasicFilterBasedRPC{
    // initialization
    map<string, i32> initial(1:string tableName, 2:map<string, list<string>> ipStrMap);

    // generate replay interval set that maybe contain matched results
    // input: variable name, query window, the head or tail marker for this variable
    // return: replay intervals
    binary getInitialIntervals(1:string varName, 2:i64 window, 3:i32 headTailMarker);

    // input: variable name, query window, dependent predicates map, shrink filter buffer
    map<string, map<i64, set<string>>> getHashTable4EQJoin(1:string varName, 2:i64 window, 3:map<string, list<string>> dpStrMap, 4:binary intervals)

    // input: variable name, query window, dependent predicates map, replay intervals
    // output: intervals
    binary windowFilter(1:string varName, 2:i64 window, 3:i32 headTailMarker, 4:binary intervals);

    // if current variable and previous variables have equal dependent predicates, then we call this function
    // input: variable name, query window, the head or tail marker for this variable, sequence map, dependent predicates map, bool filters map
    // output: intervals
    binary eqJoinFilter(1:string varName, 2:i64 window, 3:i32 headTailMarker, 4:map<string, bool> previousOrNext, 5:map<string, list<string>> dpStrMap, 6:map<string, map<i64, set<string>>> pairs)

    // send filtered events
    SDataChunk getAllFilteredEvents(1:i64 window, 2:binary intervals);
}