
// please run below command to generate java code
// thrift -gen java new_service.thrift



struct DataChunk{
    1: i32 chunkId;
    2: binary data;         // we always ensure data.length % recordSize = 0
    3: bool isLastChunk;
}

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

struct PushPullDataChunk{
    1: i32 chunkId;
    2: map<string, binary> varEventMap;
    3: bool isLastChunk;
}

struct FilteredResult{
    1: i32 filteredNum;
    2: binary updatedSWF;
}





service PushDownRPC2{
    DataChunk pullEvents(1:string clientId, 2:string tableName, 3:i32 offset, 4:map<string, list<string>> ipMap);
}

service TwoTripsRPC2{
    TwoTripsDataChunk pullBasicInfo(1:string clientId, 2:string tableName, 3:string sql, 4:i32 offset);
    DataChunk pullEvents(1:string clientId, 2:i32 offset, 3:binary intervalArray);
}

service PushPullRPC2{
    map<string, i32> initial(1:string clientId, 2:string tableName, 3:string sql)
    PushPullDataChunk getEventsByVarName(1:string clientId, 2:list<string> requestedVarNames, 3:i32 offset);
    PushPullDataChunk matchFilter(1:string clientId, 2:list<string> requestedVarNames, 3:PushPullDataChunk dataChunk, 4:i32 offset);
}

service FilterBasedRPC2{
    map<string, i32> initial(1:string clientId, 2:string tableName, 3:map<string, list<string>> ipStrMap);
    binary getInitialIntervals(1:string clientId, 2:string varName, 3:i64 window, 4:i32 headTailMarker);

    map<string, binary> getBF4EQJoin(1:string clientId, 2:string varName, 3:i64 window, 4:map<string, list<string>> dpStrMap, 5:map<string, i32> keyNumMap, 6:binary buff)
    FilteredResult windowFilter(1:string clientId, 2:string varName, 3:i64 window, 4:i32 headTailMarker, 5:binary buff);
    FilteredResult eqJoinFilter(1:string clientId, 2:string varName, 3:i64 window, 4:i32 headTailMarker, 5:map<string, bool> previousOrNext, 6:map<string, list<string>> dpStrMap, 7:map<string, binary> bfBufferMap)

    DataChunk getAllFilteredEvents(1:string clientId, 2:i64 window, 3:binary updatedSWF);
}
