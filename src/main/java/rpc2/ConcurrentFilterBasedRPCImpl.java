package rpc2;

import compute.Args;
import filter.OptimizedSWF;
import filter.BasicBF;
import filter.WindowWiseBF;
import org.apache.thrift.TException;
import parser.EqualDependentPredicate;
import rpc2.iface.DataChunk;
import rpc2.iface.FilterBasedRPC2;
import rpc2.iface.FilteredResult;
import store.EventCache;
import store.FullScan;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConcurrentFilterBasedRPCImpl implements FilterBasedRPC2.Iface {
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
        List<String> hasFilteredVarNames;
        OptimizedSWF optimizedSWF;
        List<byte[]> records;
        int offset;

        ClientState(){
            this.hasFilteredVarNames = new ArrayList<>();
            this.optimizedSWF = null;
            this.records = null;
            this.offset = 0;
        }
    }

    @Override
    public Map<String, Integer> initial(String clientId, String tableName, Map<String, List<String>> ipStrMap) throws TException {
        if(sessions.containsKey(clientId)){
            throw new TException("Session already exists for client: " + clientId);
        }else{
            sessions.put(clientId, new ClientState());
        }
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
                    state.cache = fullscan.concurrentScanBasedVarName(ipStrMap);
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
        System.out.println(qid + "-th query read cost: " + (endRead - startRead) + "ms");
        return state.cache.getCardinality();
    }

    @Override
    public ByteBuffer getInitialIntervals(String clientId, String varName, long window, int headTailMarker) throws TException {
        ClientState state = sessions.get(clientId);
        if(state == null){
            throw new TException("No session found for client: " + clientId);
        }

        ByteBuffer intervalBuffer = state.cache.generateReplayIntervals(varName, window, headTailMarker);
        state.hasFilteredVarNames.add(varName);
        return intervalBuffer;
    }

    @Override
    public Map<String, ByteBuffer> getBF4EQJoin(String clientId, String varName, long window, Map<String, List<String>> dpStrMap, Map<String, Integer> keyNumMap, ByteBuffer buff) throws TException {
        ClientState state = sessions.get(clientId);
        if(state == null){
            throw new TException("No session found for client: " + clientId);
        }

        state.optimizedSWF = OptimizedSWF.deserialize(buff);
        for(int i = 0; i < state.hasFilteredVarNames.size() - 1; i++){
            state.cache.simpleFilter(state.hasFilteredVarNames.get(i), window, state.optimizedSWF);
        }

        int dpMapSize = dpStrMap.size();
        Map<String, ByteBuffer> bfMap = new HashMap<>(dpMapSize << 1);
        for(String preVarName : dpStrMap.keySet()) {
            List<String> dpStrList = dpStrMap.get(preVarName);
            List<EqualDependentPredicate> dpList = new ArrayList<>(dpStrList.size());
            for (String dpStr : dpStrList) {
                dpList.add(new EqualDependentPredicate(dpStr));
            }
            ByteBuffer buffer = state.cache.generateBloomFilter(preVarName, keyNumMap.get(preVarName), window, dpList);
            // System.out.println("generate bf size: " + buffer.capacity() + " bytes");//debug
            bfMap.put(preVarName, buffer);
        }
        return bfMap;
    }

    @Override
    public FilteredResult windowFilter(String clientId, String varName, long window, int headTailMarker, ByteBuffer buff) throws TException {
        ClientState state = sessions.get(clientId);
        if(state == null){
            throw new TException("No session found for client: " + clientId);
        }

        state.optimizedSWF = OptimizedSWF.deserialize(buff);
        ByteBuffer ans = state.cache.updatePointers(varName, window, headTailMarker, state.optimizedSWF);
        state.hasFilteredVarNames.add(varName);
        return new FilteredResult(state.cache.getEventNum(varName), ans);
    }

    @Override
    public FilteredResult eqJoinFilter(String clientId, String varName, long window, int headTailMarker, Map<String, Boolean> previousOrNext, Map<String, List<String>> dpStrMap, Map<String, ByteBuffer> bfBufferMap) throws TException {
        ClientState state = sessions.get(clientId);
        if(state == null){
            throw new TException("No session found for client: " + clientId);
        }

        int previousVarNum = previousOrNext.size();
        Map<String, BasicBF> bfMap = new HashMap<>(previousVarNum << 1);

        Map<String, List<EqualDependentPredicate>> dpMap = new HashMap<>(previousVarNum << 1);

        for(String previousVariableName : bfBufferMap.keySet()){
            ByteBuffer buffer = bfBufferMap.get(previousVariableName);
            // WindowWiseBF or StandardBF
            BasicBF bf = WindowWiseBF.deserialize(buffer);
            bfMap.put(previousVariableName, bf);
            List<EqualDependentPredicate> dps = new ArrayList<>();
            List<String> dpStrList = dpStrMap.get(previousVariableName);
            for(String dpStr : dpStrList){
                dps.add(new EqualDependentPredicate(dpStr));
            }
            dpMap.put(previousVariableName, dps);
        }

        ByteBuffer buffer = state.cache.updatePointers(varName, window, headTailMarker, state.optimizedSWF, previousOrNext, bfMap, dpMap);

        state.hasFilteredVarNames.add(varName);
        return new FilteredResult(state.cache.getEventNum(varName), buffer);
    }

    @Override
    public DataChunk getAllFilteredEvents(String clientId, long window, ByteBuffer updatedSWF) throws TException {
        ClientState state = sessions.get(clientId);
        if(state == null){
            throw new TException("No session found for client: " + clientId);
        }
        if(state.offset == 0){
            state.optimizedSWF = OptimizedSWF.deserialize(updatedSWF);
            state.records = state.cache.getRecords(window, state.optimizedSWF);
        }

        if (state.records == null || state.records.isEmpty()) {
            return new DataChunk(-1, ByteBuffer.allocate(0), true);
        }

        int remaining = state.records.size() - state.offset;
        DataChunk dataChunk;
        int recordSize = state.records.get(0).length;
        int MAX_RECORD_NUM = Args.MAX_CHUNK_SIZE / recordSize;
        if(remaining <= MAX_RECORD_NUM){
            ByteBuffer buffer = ByteBuffer.allocate(remaining * recordSize);
            for(int i = state.offset; i < state.records.size(); i++){
                buffer.put(state.records.get(i));
            }
            buffer.flip();
            sessions.remove(clientId);
            dataChunk = new DataChunk(-1, buffer, true);
        }
        else{
            ByteBuffer buffer = ByteBuffer.allocate(MAX_RECORD_NUM * recordSize);
            for(int i = state.offset; i < state.offset + MAX_RECORD_NUM; i++){
                buffer.put(state.records.get(i));
            }
            buffer.flip();
            dataChunk = new DataChunk(-1, buffer, false);

            state.offset += MAX_RECORD_NUM;
        }
        return dataChunk;
    }
}
