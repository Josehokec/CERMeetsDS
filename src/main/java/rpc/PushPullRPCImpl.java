package rpc;

import compute.Args;
import engine.NFA;
import engine.SelectionStrategy;
import org.apache.thrift.TException;
import parser.QueryParse;
import rpc.iface.PushPullDataChunk;
import rpc.iface.PushPullRPC;
import store.EventCache;
import store.EventSchema;
import store.FullScan;
import utils.ReplayIntervals;

import java.nio.ByteBuffer;
import java.util.*;

public class PushPullRPCImpl implements PushPullRPC.Iface {
    public static int queryId = 0;
    public EventCache cache;
    private EventSchema schema;
    public int chunkId;
    public int recordSize;

    Set<String> processedVarNames;
    //  we need to update allEvents to avoid out-of-memory
    List<byte[]> allEvents;
    public QueryParse query;

    Map<String, List<byte[]>> filteredRecordsMap;

    @Override
    public Map<String, Integer> initial(String tableName, String sql) throws TException {
        System.out.println(queryId + "-th query arrives....");
        queryId++;
        query = new QueryParse(sql);

        processedVarNames = new HashSet<>();
        allEvents = new ArrayList<>(512);
        schema = EventSchema.getEventSchema(query.getTableName());

        long startRead = System.currentTimeMillis();
        FullScan fullscan = new FullScan(tableName);
        cache = fullscan.concurrentScanBasedVarName(query.getIpStringMap());
        long endRead = System.currentTimeMillis();
        System.out.println("read cost: " + (endRead - startRead) + "ms");

        // here we call sort function
        cache.sortByTimestamp();
        //initialSize = cache.getCacheSize();
        //maxSpaceSize = 0;
        return cache.getCardinality();
    }

    @Override
    public PushPullDataChunk getEventsByVarName(List<String> requestedVarNames, int offset) throws TException {
        if(offset == 0){
            chunkId = 0;
            recordSize = cache.getRecordSize();
        }

        Map<String, List<byte[]>> recordMap = new HashMap<>(requestedVarNames.size() << 1);
        Map<String, Integer> recordNumMap = new HashMap<>(requestedVarNames.size() << 1);

        for(String varName : requestedVarNames){
            // here we use offset to obtain events
            List<byte[]> records = cache.getVarByteRecords(varName, offset);
            if(!records.isEmpty()){
                recordMap.put(varName, records);
                recordNumMap.put(varName, records.size());
            }
        }
        int hasUsedSize = 0;
        List<Map.Entry<String, Integer>> entryList = new ArrayList<>(recordNumMap.entrySet());
        entryList.sort(Map.Entry.comparingByValue());

        // I like this algorithm since it is elegant
        int waitingProcessedVarNum = entryList.size();
        Map<String, ByteBuffer> varEventMap = new HashMap<>(waitingProcessedVarNum << 1);

        int maxReadOffset = -1;     // to avoid each varName has different size

        for(Map.Entry<String, Integer> entry : entryList){
            String varName = entry.getKey();
            int remainingSize = (Args.MAX_CHUNK_SIZE - hasUsedSize) / waitingProcessedVarNum;
            int consumeSize = recordMap.get(varName).size() * recordSize;
            if(consumeSize <= remainingSize){
                ByteBuffer buffer = ByteBuffer.allocate(consumeSize);
                List<byte[]> records = recordMap.get(varName);
                for(byte[] record : records){
                    buffer.put(record);
                }
                buffer.flip();
                varEventMap.put(varName, buffer);
            }else{
                if(maxReadOffset == -1){
                    maxReadOffset = remainingSize / recordSize;
                }
                ByteBuffer buffer = ByteBuffer.allocate(maxReadOffset * recordSize);
                List<byte[]> records = recordMap.get(varName);
                for(int i = 0; i < maxReadOffset; i++){
                    buffer.put(records.get(i));
                }
                buffer.flip();
                varEventMap.put(varName, buffer);
            }
        }

        return new PushPullDataChunk(chunkId++, varEventMap, maxReadOffset == -1);
    }

    @Override
    public PushPullDataChunk matchFilter(List<String> requestedVarNames, PushPullDataChunk dataChunk, int offset) throws TException {
        if(offset == 0){
            Map<String,ByteBuffer> eventListMap = dataChunk.varEventMap;
            // we need to waiting obtain all events
            for(String varName : eventListMap.keySet()){
                processedVarNames.add(varName);
                // if there has same timestamp, we may without some results
                allEvents = mergeEvents(allEvents, eventListMap.get(varName), schema);
            }

            if(dataChunk.isLastChunk){
                Set<String> dpNames = query.getDpMap().keySet();

                // start filtering...
                filteredRecordsMap = new HashMap<>();
                // hasProcessed: A   request: B & C
                boolean openOptimize = processedVarNames.size() == 1;
                String headVarName = processedVarNames.iterator().next();
                for(String requestedVarName : requestedVarNames){
                    String key = headVarName.compareTo(requestedVarName) < 0 ? (headVarName + "-" + requestedVarName) : (requestedVarName + "-" + headVarName);
                    if(openOptimize && !dpNames.contains(key)){
                        //here we use interval array to filter
                        int headOrTail = query.headTailMarker(headVarName);
                        long leftOffset;
                        long rightOffset;
                        switch (headOrTail){
                            case 0:
                                leftOffset = 0;
                                rightOffset = query.getWindow();
                                break;
                            case 1:
                                leftOffset = -query.getWindow();
                                rightOffset = 0;
                                break;
                            default:
                                leftOffset = -query.getWindow();
                                rightOffset = query.getWindow();
                        }

                        ReplayIntervals ri = new ReplayIntervals(allEvents.size());
                        for(byte[] e : allEvents){
                            long ts = schema.getTimestamp(e);
                            ri.insert(ts + leftOffset, ts + rightOffset);
                        }
                        ri.sortAndReconstruct();

                        // filter using interval array
                        List<byte[]> records = cache.getVarByteRecords(requestedVarName, 0);
                        List<byte[]> filteredRecords = new ArrayList<>(records.size() >> 3);

                        for(byte[] e : records){
                            long ts = schema.getTimestamp(e);
                            if(ri.contains(ts)){
                                filteredRecords.add(e);
                            }
                        }
                        filteredRecordsMap.put(requestedVarName, filteredRecords);
                        processedVarNames.add(requestedVarName);

                        // maxSpaceSize = Math.max(maxSpaceSize, ri.getIntervals().size() * 16 + allEvents.size() * recordSize);
                        if(!schema.getTableName().equals("SYNTHETIC")){
                            cache.cleanByName(requestedVarName);
                        }
                    }
                    else{
                        Set<String> projectedVarNames = new HashSet<>(processedVarNames);
                        projectedVarNames.add(requestedVarName);
                        // we need to get all events
                        List<byte[]> requestRecords = cache.getVarByteRecords(requestedVarName, 0); // 44, 92????
                        List<byte[]> records = cache.mergeEvents(allEvents, requestRecords);
                        NFA nfa = new NFA();
                        nfa.constructNFA(query, projectedVarNames);
                        for(byte[] record : records){
                            nfa.consume(record, SelectionStrategy.SKIP_TILL_ANY_MATCH, schema);
                        }
                        List<byte[]> projectedResults = nfa.getProjectedRecords(requestedVarName);
                        filteredRecordsMap.put(requestedVarName, projectedResults);
                        if(!schema.getTableName().equals("SYNTHETIC")){
                            cache.cleanByName(requestedVarName);
                        }

                        // last variable, only synthetic dataset allow overlap
                        if(processedVarNames.size() == query.getVariableNames().size() - 1){
                            allEvents = null;
                            cache = null;
                            System.gc();
                        }
                    }
                }
            }else{
                // response null info
                return new PushPullDataChunk(-1, null, false);
            }
        }

        // we need to transmit filteredRecordsMap
        Map<String, Integer> recordNumMap = new HashMap<>();
        for(Map.Entry<String, List<byte[]>> entry : filteredRecordsMap.entrySet()){
            int curSize = entry.getValue().size();
            if(curSize > offset){
                recordNumMap.put(entry.getKey(), entry.getValue().size());
            }
        }
        List<Map.Entry<String, Integer>> entryList = new ArrayList<>(recordNumMap.entrySet());
        entryList.sort(Map.Entry.comparingByValue());

        int hasUsedSize = 0;

        int waitingProcessedVarNum = entryList.size();
        Map<String, ByteBuffer> varEventMap = new HashMap<>(waitingProcessedVarNum << 1);

        int maxReadOffset = -1;     // to avoid each varName has different size

        for(Map.Entry<String, Integer> entry : entryList){
            String varName = entry.getKey();
            int remainingSize = (Args.MAX_CHUNK_SIZE - hasUsedSize) / waitingProcessedVarNum;
            int consumeSize = (filteredRecordsMap.get(varName).size() - offset) * recordSize;
            if(consumeSize <= remainingSize){
                ByteBuffer buffer = ByteBuffer.allocate(consumeSize);
                List<byte[]> records = filteredRecordsMap.get(varName);
                for(int i = offset; i < records.size(); i++){
                    byte[] record = records.get(i);
                    buffer.put(record);
                }
                buffer.flip();
                varEventMap.put(varName, buffer);
            }else{
                if(maxReadOffset == -1){
                    maxReadOffset = remainingSize / recordSize;
                }
                ByteBuffer buffer = ByteBuffer.allocate(maxReadOffset * recordSize);
                List<byte[]> records = filteredRecordsMap.get(varName);
                for(int i = offset; i < offset + maxReadOffset; i++){
                    buffer.put(records.get(i));
                }
                buffer.flip();
                varEventMap.put(varName, buffer);
            }
        }

        return new PushPullDataChunk(chunkId++, varEventMap, maxReadOffset == -1);
    }


    public static List<byte[]> mergeEvents(List<byte[]> allEvents, ByteBuffer eventBuffer, EventSchema schema){
        // without any duplicated records
        int bufferSize = eventBuffer.remaining();
        int dataLen = schema.getFixedRecordLen();
        int recordNum = bufferSize / dataLen;

        List<byte[]> curEvents = new ArrayList<>(recordNum);

        for(int i = 0; i < recordNum; i++){
            byte[] record = new byte[dataLen];
            eventBuffer.get(record);
            curEvents.add(record);
        }

        if(allEvents.isEmpty()){
            return curEvents;
        }else{
            int size1 = allEvents.size();
            List<byte[]> mergedList = new ArrayList<>(size1 + recordNum);

            int i = 0, j = 0;
            while(i < size1 && j < recordNum){

                byte[] record1 = allEvents.get(i);
                long ts1 = schema.getTimestamp(record1);

                byte[] record2 = curEvents.get(j);
                long ts2 = schema.getTimestamp(record2);
                if(ts1 <= ts2){
                    mergedList.add(record1);
                    i++;
                }else {
                    mergedList.add(record2);
                    j++;
                }
            }

            while(i < size1){
                mergedList.add(allEvents.get(i));
                i++;
            }

            while(j < recordNum){
                mergedList.add(curEvents.get(j));
                j++;
            }

            return mergedList;
        }
    }

}
