package rpc2;

import compute.Args;
import org.apache.thrift.TException;
import parser.QueryParse;
import rpc2.iface.TsAttrPair;
import rpc2.iface.DataChunk;
import rpc2.iface.TwoTripsDataChunk;
import rpc2.iface.TwoTripsRPC2;
import store.EventCache;
import store.FullScan;
import utils.ReplayIntervals;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConcurrentTwoTripsRPCImpl implements TwoTripsRPC2.Iface {

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

    private static class ClientState {
        // You can add fields here to maintain state per client
        EventCache cache;
        Map<String, List<TsAttrPair>> pairMap;
        public int chunkId;
        public List<byte[]> filteredRecords;
        public int recordSize;
        public int MAX_RECORD_NUM;

        ClientState() {
            this.chunkId = 0;
            this.filteredRecords = new ArrayList<>();;
            this.recordSize = 0;
            this.MAX_RECORD_NUM = 10_000;
            this.pairMap = new HashMap<>();
        }

    }

    @Override
    public TwoTripsDataChunk pullBasicInfo(String clientId, String tableName, String sql, int offset) throws TException {
        if(offset == 0){
            sessions.remove(clientId);
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

        Map<String, ByteBuffer> intervalMap = null;
        int hasUsedSize = 0;

        if(offset == 0){
            int qid = queryId.getAndIncrement();
            System.out.println(clientId + ", " + qid + "-th query arrives");
            state.chunkId = 0;

            state.pairMap = new HashMap<>();
            state.filteredRecords = new ArrayList<>();

            QueryParse query = new QueryParse(sql);
            Map<String, List<String>> ipStrMap = query.getIpStringMap();
            Set<String> varAttrNameSet  = query.getVarAttrSetFromDpList();

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
                        System.err.println("OOM during concurrentScan, attempt " + attempt + ", sleeping " + 100 + "ms");
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
            System.out.println(clientId + ", " + qid + "-th query read cost: " + (endRead - startRead) + "ms");

            if(state.cache == null){
                throw new TException("Failed to perform FullScan due to repeated OOM");
            }


            // generate replay interval set
            long window = query.getWindow();
            List<String> varNames = query.getVariableNames();
            intervalMap = new HashMap<>(varNames.size() << 1);

            for(String varName : varNames){
                ByteBuffer buffer = state.cache.generateReplayIntervals(varName,window, query.headTailMarker(varName));
                intervalMap.put(varName, buffer);
                hasUsedSize += buffer.capacity();
            }

            for(String varAttrName : varAttrNameSet){
                String[] splits = varAttrName.split("-");
                List<TsAttrPair> tsAttrPairList = new ArrayList<>(state.cache.getTsAttrPairSet2(splits[0], splits[1]));
                state.pairMap.put(varAttrName, new ArrayList<>(tsAttrPairList));
            }
        }

        Map<String, Set<TsAttrPair>> partialPairs = new HashMap<>(state.pairMap.size() << 1);
        int batchNum = 512;
        for(String key : state.pairMap.keySet()){
            partialPairs.put(key, new HashSet<>(batchNum << 1));
        }

        boolean hasReadAll = false;
        do{
            boolean read = false;
            for(String key : state.pairMap.keySet()){
                List<TsAttrPair> list = state.pairMap.get(key);
                int size = list.size();
                if(offset < size){
                    if(size - offset < batchNum){
                        partialPairs.get(key).addAll(list.subList(offset, size));
                        hasUsedSize += (size - offset) * (list.get(offset).attrValue.length() + 8);
                    }else{
                        partialPairs.get(key).addAll(list.subList(offset, offset + batchNum));
                        hasUsedSize += batchNum * (list.get(offset).attrValue.length() + 8);
                    }
                    read = true;
                }
            }
            if(!read){
                hasReadAll = true;
            }
            offset += batchNum;
        }while(hasUsedSize < Args.MAX_CHUNK_SIZE && !hasReadAll);

        return new TwoTripsDataChunk(state.chunkId++, intervalMap, partialPairs, hasReadAll);
    }

    @Override
    public DataChunk pullEvents(String clientId, int offset, ByteBuffer intervalArray) throws TException {
        ClientState state = sessions.get(clientId);
        if(state == null){
            throw new TException("No session found for client: " + clientId);
        }

        if(offset == 0){
            ReplayIntervals replayIntervals = ReplayIntervals.deserialize(intervalArray);
            state.filteredRecords = state.cache.getRecords(replayIntervals);

            // we can clear the cache
            state.cache = null;
            state.chunkId = 0;

            if(state.filteredRecords == null || state.filteredRecords.isEmpty()){
                return new DataChunk(-1, ByteBuffer.allocate(0), true);
            }

            state.recordSize = state.filteredRecords.get(0).length;
            state.MAX_RECORD_NUM = Args.MAX_CHUNK_SIZE / state.recordSize;
        }

        int remaining = state.filteredRecords.size() - offset;

        DataChunk dataChunk;

        if(remaining <= state.MAX_RECORD_NUM){
            ByteBuffer buffer = ByteBuffer.allocate(remaining * state.recordSize);
            for(int i = offset; i < state.filteredRecords.size(); i++){
                buffer.put(state.filteredRecords.get(i));
            }
            buffer.flip();
            dataChunk = new DataChunk(state.chunkId, buffer, true);
        }else{
            ByteBuffer buffer = ByteBuffer.allocate(state.MAX_RECORD_NUM * state.recordSize);
            for(int i = offset; i < offset + state.MAX_RECORD_NUM; i++){
                buffer.put(state.filteredRecords.get(i));
            }
            buffer.flip();
            dataChunk = new DataChunk(state.chunkId++, buffer, false);
        }
        return dataChunk;
    }
}
