
// please run below command to generate java code
// thrift -gen java serve.thrift


// @@@Start...
struct DataChunk{
    1: i32 chunkId;
    2: binary data;         // we always ensure data.length % recordSize = 0
    3: bool isLastChunk;
}
service PushDownRPC{
    DataChunk pullEvents(1:string tableName, 2:i32 offset, 3:map<string, list<string>> ipMap);
}
// @@@End...

// @@@Start...

struct TsAttrPair{
    1: i64 timestamp;
    2: string attrValue;
}
struct TwoTripsDataChunk{
    1: i32 chunkId;
    2: map<string, binary> intervalMap;
    3: map<string, set<TsAttrPair>> pairMap;   // key is variable-attrName
    4: bool isLastChunk;
}
service TwoTripsRPC{
    TwoTripsDataChunk pullBasicInfo(1:string tableName, 2:string sql, 3:i32 offset);
    DataChunk pullEvents(1:i32 offset, 2:binary intervalArray);
}
// @@@End...

// @@@Start...
struct PushPullDataChunk{
    1: i32 chunkId;
    2: map<string, binary> varEventMap;
    3: bool isLastChunk;
}
service PushPullRPC{
    map<string, i32> initial(1:string tableName, 2:string sql)
    PushPullDataChunk getEventsByVarName(1:list<string> requestedVarNames, 2:i32 offset); // given a variable name, return its all events
    PushPullDataChunk matchFilter(1:list<string> requestedVarNames, 2:PushPullDataChunk dataChunk, 3:i32 offset); // based on request variable names, to get all related events
}
// @@@End...