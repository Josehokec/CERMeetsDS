package rpc;


import compute.Args;
import org.apache.thrift.TException;
import parser.QueryParse;
import rpc.iface.DataChunk;
import rpc.iface.TsAttrPair;
import rpc.iface.TwoTripsDataChunk;
import rpc.iface.TwoTripsRPC;
import store.EventCache;
import store.FullScan;
import utils.ReplayIntervals;

import java.nio.ByteBuffer;
import java.util.*;

public class TwoTripsRPCImpl implements TwoTripsRPC.Iface {
    public static int queryId = 0;
    public EventCache cache;
    Map<String, List<TsAttrPair>> pairMap;
    public int chunkId;

    public List<byte[]> filteredRecords;
    public int recordSize;
    public int MAX_RECORD_NUM;

    @Override
    public TwoTripsDataChunk pullBasicInfo(String tableName, String sql, int offset) throws TException {
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
            FullScan fullscan = new FullScan(tableName);
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
    public DataChunk pullEvents(int offset, ByteBuffer intervalArray) throws TException {
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
}
