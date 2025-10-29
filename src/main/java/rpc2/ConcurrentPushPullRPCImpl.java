package rpc2;


import compute.Args;
import engine.NFA;
import engine.SelectionStrategy;
import org.apache.thrift.TException;
import parser.QueryParse;
import rpc2.iface.PushPullDataChunk;
import rpc2.iface.PushPullRPC2;
import store.EventCache;
import store.EventSchema;
import store.FullScan;
import utils.ReplayIntervals;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConcurrentPushPullRPCImpl implements PushPullRPC2.Iface {
    // scan lock to avoid multiple scans at the same time
    private final AtomicBoolean scanLock = new AtomicBoolean(false);

    private boolean isScanLocked() {
        return scanLock.get();
    }

    private boolean tryAcquireScanLock() {
        return scanLock.compareAndSet(false, true);
    }

    private void releaseScanLock() {
        scanLock.set(false);
    }

    public static final AtomicInteger queryId = new AtomicInteger(0);
    private final ConcurrentHashMap<String, ClientState> sessions = new ConcurrentHashMap<>();

    private static class ClientState{
        EventCache cache;
        EventSchema schema;
        int chunkId;
        int recordSize;
        Set<String> processedVarNames;
        List<byte[]> allEvents;
        QueryParse query;
        Map<String, List<byte[]>> filteredRecordsMap;

        ClientState(){
            this.chunkId = 0;
            this.recordSize = 0;
            this.processedVarNames = new HashSet<>();
            this.allEvents = null;
            this.filteredRecordsMap = new HashMap<>();
        }
    }

    @Override
    public Map<String, Integer> initial(String clientId, String tableName, String sql) throws TException {
        sessions.remove(clientId);
        sessions.put(clientId, new ClientState());

        ClientState state = sessions.get(clientId);

        while (isScanLocked()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TException("Interrupted while waiting for scan lock");
            }
        }

        int qid = queryId.getAndIncrement();
        System.out.println(clientId + "," + qid + "-th query arrives");

        state.query = new QueryParse(sql);

        state.processedVarNames = new HashSet<>();
        state.allEvents = new ArrayList<>(512);
        state.schema = EventSchema.getEventSchema(state.query.getTableName());

        while(!ConcurrentPushDownRPCImpl.canRead()){
            System.out.println("Waiting for enough memory to do full scan...");
            System.gc();
            try {Thread.sleep(100);}
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        while (!tryAcquireScanLock()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TException("Interrupted while acquiring scan lock");
            }
        }

        long startRead = System.currentTimeMillis();
        int maxRetries = 10;
        long sleepMs = 100;

        try{
            for (int attempt = 0; attempt < maxRetries; attempt++) {
                FullScan fullscan = null;
                try {
                    fullscan = new FullScan(tableName);
                    state.cache = fullscan.concurrentScanBasedVarName(state.query.getIpStringMap());
                    break;
                } catch (OutOfMemoryError oom) {
                    System.err.println("OOM during concurrentScan, attempt " + attempt + ", sleeping " + sleepMs + "ms");
                    fullscan = null;
                    state.cache = null;
                    System.gc();
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    sleepMs = Math.min(sleepMs * 2, 5000L);
                }
            }
        }finally {
            releaseScanLock();
        }


        long endRead = System.currentTimeMillis();
        System.out.println("read cost: " + (endRead - startRead) + "ms");

        state.cache.sortByTimestamp();
        return state.cache.getCardinality();

    }

    @Override
    public PushPullDataChunk getEventsByVarName(String clientId, List<String> requestedVarNames, int offset) throws TException {

        ClientState state = sessions.get(clientId);
        if(state == null){
            throw new TException("No session found for client: " + clientId);
        }

        if(offset == 0){
            state.chunkId = 0;
            state.recordSize = state.cache.getRecordSize();
        }

        Map<String, List<byte[]>> recordMap = new HashMap<>(requestedVarNames.size() << 1);
        Map<String, Integer> recordNumMap = new HashMap<>(requestedVarNames.size() << 1);

        for(String varName : requestedVarNames){
            // here we use offset to obtain events
            List<byte[]> records = state.cache.getVarByteRecords(varName, offset);
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
            int consumeSize = recordMap.get(varName).size() * state.recordSize;
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
                    maxReadOffset = remainingSize / state.recordSize;
                }
                ByteBuffer buffer = ByteBuffer.allocate(maxReadOffset * state.recordSize);
                List<byte[]> records = recordMap.get(varName);
                for(int i = 0; i < maxReadOffset; i++){
                    buffer.put(records.get(i));
                }
                buffer.flip();
                varEventMap.put(varName, buffer);
            }
        }

        return new PushPullDataChunk(state.chunkId++, varEventMap, maxReadOffset == -1);
    }

    @Override
    public PushPullDataChunk matchFilter(String clientId, List<String> requestedVarNames, PushPullDataChunk dataChunk, int offset) throws TException {
        ClientState state = sessions.get(clientId);
        if(state == null){
            throw new TException("No session found for client: " + clientId);
        }

        if(offset == 0){
            Map<String,ByteBuffer> eventListMap = dataChunk.varEventMap;
            // we need to waiting obtain all events
            for(String varName : eventListMap.keySet()){
                state.processedVarNames.add(varName);
                // if there has same timestamp, we may without some results
                state.allEvents = mergeEvents(state.allEvents, eventListMap.get(varName), state.schema);
            }

            if(dataChunk.isLastChunk){
                Set<String> dpNames = state.query.getDpMap().keySet();

                // start filtering...
                state.filteredRecordsMap = new HashMap<>();
                // hasProcessed: A   request: B & C
                boolean openOptimize = state.processedVarNames.size() == 1;
                String headVarName = state.processedVarNames.iterator().next();
                for(String requestedVarName : requestedVarNames){
                    String key = headVarName.compareTo(requestedVarName) < 0 ? (headVarName + "-" + requestedVarName) : (requestedVarName + "-" + headVarName);
                    if(openOptimize && !dpNames.contains(key)){
                        //here we use interval array to filter
                        int headOrTail = state.query.headTailMarker(headVarName);
                        long leftOffset;
                        long rightOffset;
                        switch (headOrTail){
                            case 0:
                                leftOffset = 0;
                                rightOffset = state.query.getWindow();
                                break;
                            case 1:
                                leftOffset = -state.query.getWindow();
                                rightOffset = 0;
                                break;
                            default:
                                leftOffset = -state.query.getWindow();
                                rightOffset = state.query.getWindow();
                        }

                        ReplayIntervals ri = new ReplayIntervals(state.allEvents.size());
                        for(byte[] e : state.allEvents){
                            long ts = state.schema.getTimestamp(e);
                            ri.insert(ts + leftOffset, ts + rightOffset);
                        }
                        ri.sortAndReconstruct();

                        // filter using interval array
                        List<byte[]> records = state.cache.getVarByteRecords(requestedVarName, 0);
                        List<byte[]> filteredRecords = new ArrayList<>(records.size() >> 3);

                        for(byte[] e : records){
                            long ts = state.schema.getTimestamp(e);
                            if(ri.contains(ts)){
                                filteredRecords.add(e);
                            }
                        }
                        state.filteredRecordsMap.put(requestedVarName, filteredRecords);
                        state.processedVarNames.add(requestedVarName);

                        // maxSpaceSize = Math.max(maxSpaceSize, ri.getIntervals().size() * 16 + allEvents.size() * recordSize);
                        if(!state.schema.getTableName().equals("SYNTHETIC")){
                            state.cache.cleanByName(requestedVarName);
                        }
                    }
                    else{
                        Set<String> projectedVarNames = new HashSet<>(state.processedVarNames);
                        projectedVarNames.add(requestedVarName);
                        // we need to get all events
                        List<byte[]> requestRecords = state.cache.getVarByteRecords(requestedVarName, 0); // 44, 92????
                        List<byte[]> records = state.cache.mergeEvents(state.allEvents, requestRecords);
                        NFA nfa = new NFA();
                        nfa.constructNFA(state.query, projectedVarNames);
                        for(byte[] record : records){
                            nfa.consume(record, SelectionStrategy.SKIP_TILL_ANY_MATCH, state.schema);
                        }
                        List<byte[]> projectedResults = nfa.getProjectedRecords(requestedVarName);
                        state.filteredRecordsMap.put(requestedVarName, projectedResults);
                        if(!state.schema.getTableName().equals("SYNTHETIC")){
                            state.cache.cleanByName(requestedVarName);
                        }

                        // last variable, only synthetic dataset allow overlap
                        if(state.processedVarNames.size() == state.query.getVariableNames().size() - 1){
                            state.allEvents = null;
                            state.cache = null;
                            System.gc();
                        }
                    }
                }
            }
            else{
                // response null info
                return new PushPullDataChunk(-1, null, false);
            }
        }

        // we need to transmit filteredRecordsMap
        Map<String, Integer> recordNumMap = new HashMap<>();
        for(Map.Entry<String, List<byte[]>> entry : state.filteredRecordsMap.entrySet()){
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
            int consumeSize = (state.filteredRecordsMap.get(varName).size() - offset) * state.recordSize;
            if(consumeSize <= remainingSize){
                ByteBuffer buffer = ByteBuffer.allocate(consumeSize);
                List<byte[]> records = state.filteredRecordsMap.get(varName);
                for(int i = offset; i < records.size(); i++){
                    byte[] record = records.get(i);
                    buffer.put(record);
                }
                buffer.flip();
                varEventMap.put(varName, buffer);
            }else{
                if(maxReadOffset == -1){
                    maxReadOffset = remainingSize / state.recordSize;
                }
                ByteBuffer buffer = ByteBuffer.allocate(maxReadOffset * state.recordSize);
                List<byte[]> records = state.filteredRecordsMap.get(varName);
                for(int i = offset; i < offset + maxReadOffset; i++){
                    buffer.put(records.get(i));
                }
                buffer.flip();
                varEventMap.put(varName, buffer);
            }
        }

        return new PushPullDataChunk(state.chunkId++, varEventMap, maxReadOffset == -1);
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
