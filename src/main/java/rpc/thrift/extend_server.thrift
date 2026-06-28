// please run below command to generate java code
// thrift -gen java extend_server.thrift

struct ExtendFilteredResult{
    1: i32 filteredNum;
    2: binary updatedSWF;
}

struct ExtendDataChunk{
    1: i32 chunkId;
    2: binary data;         // we always ensure data.length % recordSize = 0
    3: bool isLastChunk;
}

service ExtendFilterBasedRPC{
    // same interface as FilterBasedRPC
    map<string, i32> initial(1:string tableName, 2:map<string, list<string>> ipStrMap);
    binary getInitialIntervals(1:string varName, 2:i64 window, 3:i32 headTailMarker);
    map<string, binary> getBF4EQJoin(1:string varName, 2:i64 window, 3:map<string, list<string>> dpStrMap, 4:map<string, i32> keyNumMap, 5:binary buff)
    ExtendFilteredResult windowFilter(1:string varName, 2:i64 window, 3:i32 headTailMarker, 4:binary buff);
    ExtendFilteredResult eqJoinFilter(1:string varName, 2:i64 window, 3:i32 headTailMarker, 4:map<string, bool> previousOrNext, 5:map<string, list<string>> dpStrMap, 6:map<string, binary> bfBufferMap);
    ExtendDataChunk getAllFilteredEvents(1:i64 window, 2:binary updatedSWF);

    // new interface for extend filter-based RPC
    map<string, i32> extendInitial(1:string tableName, 2:map<string, list<string>> ipStrMap, 3: binary intervalBuff);
}
