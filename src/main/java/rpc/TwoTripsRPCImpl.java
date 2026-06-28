package rpc;


import compute.Args;
import org.apache.thrift.TException;
import parser.QueryParse;
import rpc.iface.DataChunk;
import rpc.iface.TsAttrPair;
import rpc.iface.TwoTripsDataChunk;
import rpc.iface.TwoTripsRPC;
import store.EventCache;
import store.EventStore;
import store.FullScan;
import utils.ReplayIntervals;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

public class TwoTripsRPCImpl implements TwoTripsRPC.Iface {
    private static final int DISABLE_COMPUTE_CACHE_OFFSET = Integer.MIN_VALUE;
    private static final String TWO_TRIPS_CACHE_PAGE_KEY = "__compute_cache_pages__";

    public static int queryId = 0;
    public EventCache cache;
    Map<String, List<TsAttrPair>> pairMap;
    public int chunkId;

    public List<byte[]> filteredRecords;
    public int recordSize;
    public int MAX_RECORD_NUM;
    private File cachePageTransferFile;
    private long cachePageTransferOffset;
    private long cachePageTransferLimit;
    private String computeCacheTableName;
    private int computeCachePageNum;

    @Override
    public synchronized TwoTripsDataChunk pullBasicInfo(String tableName, String sql, int offset) throws TException {
        if(offset == DISABLE_COMPUTE_CACHE_OFFSET){
            disableComputeCache(tableName);
            return new TwoTripsDataChunk(-1, new HashMap<String, ByteBuffer>(0),
                    new HashMap<String, Set<TsAttrPair>>(0), true);
        }
        if(offset < 0){
            return getComputeCachePages(tableName, -(long) offset);
        }

        Map<String, ByteBuffer> intervalMap = null;
        int hasUsedSize = 0;

        if(offset == 0){
            System.out.println(queryId + "-th query arrives.....");
            queryId++;
            chunkId = 0;

            pairMap = new HashMap<>();
            filteredRecords = new ArrayList<>();

            QueryParse query = new QueryParse(sql);
            Map<String, List<String>> ipStrMap = query.getIpStringMap();
            Set<String> varAttrNameSet  = query.getVarAttrSetFromDpList();

            long startRead = System.currentTimeMillis();
            int skipPageNum = getComputeCachePageNum(tableName);
            if(skipPageNum > 0){
                System.out.println("skip compute-cached leading pages during two-trips scan: " + skipPageNum);
            }
            FullScan fullscan = new FullScan(tableName, skipPageNum);
            cache = fullscan.concurrentScanBasedVarName(ipStrMap);
            long endRead = System.currentTimeMillis();
            System.out.println("read cost: " + (endRead - startRead) + "ms");

            // generate replay interval set
            long window = query.getWindow();
            List<String> varNames = query.getVariableNames();
            intervalMap = new HashMap<>(varNames.size() << 1);

            for(String varName : varNames){
                ByteBuffer buffer = cache.generateReplayIntervals(varName,window, query.headTailMarker(varName));
                intervalMap.put(varName, buffer);
                hasUsedSize += buffer.capacity();
            }

            for(String varAttrName : varAttrNameSet){
                String[] splits = varAttrName.split("-");
                List<TsAttrPair> tsAttrPairList = new ArrayList<>(cache.getTsAttrPairSet(splits[0], splits[1]));
                pairMap.put(varAttrName, new ArrayList<>(tsAttrPairList));
            }
        }

        Map<String, Set<TsAttrPair>> partialPairs = new HashMap<>(pairMap.size() << 1);
        int batchNum = 512;
        for(String key : pairMap.keySet()){
            partialPairs.put(key, new HashSet<>(batchNum << 1));
        }

        boolean hasReadAll = false;
        do{
            boolean read = false;
            for(String key : pairMap.keySet()){
                List<TsAttrPair> list = pairMap.get(key);
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

        return new TwoTripsDataChunk(chunkId++, intervalMap, partialPairs, hasReadAll);
    }

    @Override
    public synchronized DataChunk pullEvents(int offset, ByteBuffer intervalArray) throws TException {
        if(offset == 0){
            ReplayIntervals replayIntervals = ReplayIntervals.deserialize(intervalArray);
            filteredRecords = cache.getRecords(replayIntervals);

            // we can clear the cache
            cache = null;
            chunkId = 0;

            if(filteredRecords == null || filteredRecords.isEmpty()){
                return new DataChunk(-1, ByteBuffer.allocate(0), true);
            }

            recordSize = filteredRecords.get(0).length;
            MAX_RECORD_NUM = Args.MAX_CHUNK_SIZE / recordSize;
        }

        int remaining = filteredRecords.size() - offset;

        DataChunk dataChunk;
        if(remaining <= MAX_RECORD_NUM){
            ByteBuffer buffer = ByteBuffer.allocate(remaining * recordSize);
            for(int i = offset; i < filteredRecords.size(); i++){
                buffer.put(filteredRecords.get(i));
            }
            buffer.flip();
            dataChunk = new DataChunk(chunkId, buffer, true);
        }else{
            ByteBuffer buffer = ByteBuffer.allocate(MAX_RECORD_NUM * recordSize);
            for(int i = offset; i < offset + MAX_RECORD_NUM; i++){
                buffer.put(filteredRecords.get(i));
            }
            buffer.flip();
            dataChunk = new DataChunk(chunkId++, buffer, false);
        }
        return dataChunk;
    }

    private TwoTripsDataChunk getComputeCachePages(String tableName, long cachePageNum) throws TException {
        if(cachePageTransferFile == null || !tableName.equals(computeCacheTableName)){
            EventStore store = new EventStore(tableName + System.getProperty("nodeId", "_0"), false);
            cachePageTransferFile = store.getFile();
            cachePageTransferOffset = 0;
            long requestedBytes = cachePageNum > Long.MAX_VALUE / EventStore.pageSize ?
                    Long.MAX_VALUE : cachePageNum * (long) EventStore.pageSize;
            cachePageTransferLimit = Math.min(store.getFileSize(), requestedBytes);
            computeCacheTableName = tableName;
            long cachedPages = (cachePageTransferLimit + EventStore.pageSize - 1) / EventStore.pageSize;
            computeCachePageNum = (int) Math.min(Integer.MAX_VALUE, cachedPages);
            System.out.println("two-trips compute cache configured by compute node: tableName=" + tableName
                    + ", requestedPages=" + cachePageNum
                    + ", cachedPages=" + computeCachePageNum
                    + ", cachedBytes=" + cachePageTransferLimit);
        }

        if(cachePageTransferFile == null || cachePageTransferOffset >= cachePageTransferLimit){
            resetCachePageTransfer();
            Map<String, ByteBuffer> emptyMap = new HashMap<String, ByteBuffer>(0);
            return new TwoTripsDataChunk(-1, emptyMap, new HashMap<String, Set<TsAttrPair>>(0), true);
        }

        int readSize = (int) Math.min(Args.MAX_CHUNK_SIZE, cachePageTransferLimit - cachePageTransferOffset);
        byte[] bytes = new byte[readSize];
        int readOffset = 0;
        try(RandomAccessFile raf = new RandomAccessFile(cachePageTransferFile, "r")){
            raf.seek(cachePageTransferOffset);
            while(readOffset < readSize){
                int read = raf.read(bytes, readOffset, readSize - readOffset);
                if(read == -1){
                    break;
                }
                readOffset += read;
            }
        }catch (Exception e){
            throw new TException("Failed to transfer two-trips compute cache pages from "
                    + cachePageTransferFile.getAbsolutePath(), e);
        }

        cachePageTransferOffset += readOffset;
        boolean isLastChunk = cachePageTransferOffset >= cachePageTransferLimit || readOffset < readSize;
        Map<String, ByteBuffer> intervalMap = new HashMap<String, ByteBuffer>(2);
        intervalMap.put(TWO_TRIPS_CACHE_PAGE_KEY, ByteBuffer.wrap(bytes, 0, readOffset));
        if(isLastChunk){
            resetCachePageTransfer();
        }
        return new TwoTripsDataChunk(-1, intervalMap, new HashMap<String, Set<TsAttrPair>>(0), isLastChunk);
    }

    private void resetCachePageTransfer(){
        cachePageTransferFile = null;
        cachePageTransferOffset = 0;
        cachePageTransferLimit = 0;
    }

    private void disableComputeCache(String tableName) {
        computeCacheTableName = tableName;
        computeCachePageNum = 0;
        resetCachePageTransfer();
        System.out.println("two-trips compute cache disabled by compute node: tableName=" + computeCacheTableName);
    }

    private int getComputeCachePageNum(String tableName) {
        if(computeCacheTableName == null || !computeCacheTableName.equals(tableName)){
            return 0;
        }
        return computeCachePageNum;
    }
}
