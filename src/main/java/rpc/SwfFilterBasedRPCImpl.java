package rpc;

import compute.Args;
import filter.OptimizedSWF;
import org.apache.thrift.TException;
import parser.EqualDependentPredicate;
import rpc.iface.BasicFilterBasedRPC;
import rpc.iface.SDataChunk;
import store.EventCache;
import store.FullScan;

import java.nio.ByteBuffer;
import java.util.*;

// additional implementation for SWF-based multi-round filtering
public class SwfFilterBasedRPCImpl implements BasicFilterBasedRPC.Iface{

    private EventCache cache;
    private List<String> hasFilteredVarNames;
    private OptimizedSWF optimizedSWF;

    public static int queryId = 0;
    public List<byte[]> records;
    public int offset;

    @Override
    public Map<String, Integer> initial(String tableName, Map<String, List<String>> ipStrMap) throws TException {
        System.out.println(queryId + "-th query arrives...");
        queryId++;
        records = null;
        offset = 0;

        hasFilteredVarNames = new ArrayList<>(8);
        optimizedSWF = null;

        long startRead = System.currentTimeMillis();
        FullScan fullscan = new FullScan(tableName);
        cache = fullscan.concurrentScanBasedVarName(ipStrMap);
        long endRead = System.currentTimeMillis();
        System.out.println("read cost: " + (endRead - startRead) + "ms");
        return cache.getCardinality();
    }

    @Override
    public ByteBuffer getInitialIntervals(String varName, long window, int headTailMarker) throws TException {
        ByteBuffer intervalBuffer = cache.generateReplayIntervals(varName, window, headTailMarker);
        hasFilteredVarNames.add(varName);
        return intervalBuffer;
    }

    @Override
    public Map<String, Map<Long, Set<String>>> getHashTable4EQJoin(String varName, long window, Map<String, List<String>> dpStrMap, ByteBuffer buff) throws TException {
        optimizedSWF = OptimizedSWF.deserialize(buff);
        for(int i = 0; i < hasFilteredVarNames.size() - 1; i++){
            cache.simpleFilter(hasFilteredVarNames.get(i), window, optimizedSWF);
        }

        int dpMapSize = dpStrMap.size();
        Map<String, Map<Long, Set<String>>> ans = new HashMap<>(dpMapSize << 1);
        for(String preVarName : dpStrMap.keySet()) {
            List<String> dpStrList = dpStrMap.get(preVarName);
            List<EqualDependentPredicate> dpList = new ArrayList<>(dpStrList.size());
            for (String dpStr : dpStrList) {
                dpList.add(new EqualDependentPredicate(dpStr));
            }
            Map<Long, Set<String>> pairSet = cache.generatePairSet(preVarName, window, dpList);
            ans.put(preVarName, pairSet);
        }

        return ans;
    }

    @Override
    public ByteBuffer windowFilter(String varName, long window, int headTailMarker, ByteBuffer buff) throws TException {
        optimizedSWF = OptimizedSWF.deserialize(buff);
        ByteBuffer updatedMarkers = cache.updatePointers(varName, window, headTailMarker, optimizedSWF);
        hasFilteredVarNames.add(varName);
        return updatedMarkers;
    }

    @Override
    public ByteBuffer eqJoinFilter(String varName, long window, int headTailMarker, Map<String, Boolean> previousOrNext, Map<String, List<String>> dpStrMap, Map<String, Map<Long, Set<String>>> pairs) throws TException {
        long startTime = System.currentTimeMillis();
        int previousVarNum = previousOrNext.size();
        Map<String, List<EqualDependentPredicate>> dpMap = new HashMap<>(previousVarNum << 1);

        for(String previousVariableName : pairs.keySet()){
            List<EqualDependentPredicate> dps = new ArrayList<>();
            List<String> dpStrList = dpStrMap.get(previousVariableName);
            for(String dpStr : dpStrList){
                dps.add(new EqualDependentPredicate(dpStr));
            }
            dpMap.put(previousVariableName, dps);
        }

        ByteBuffer updatedMarkers = cache.updatePointers3(varName, window, headTailMarker, optimizedSWF, previousOrNext, pairs, dpMap);

        hasFilteredVarNames.add(varName);

        long endTime = System.currentTimeMillis();
        System.out.println("varName: " + varName + " filter cost: " + (endTime - startTime) + "ms");
        return updatedMarkers;
    }

    @Override
    public SDataChunk getAllFilteredEvents(long window, ByteBuffer intervals) throws TException {
        if(offset == 0){
            optimizedSWF = OptimizedSWF.deserialize(intervals);
            records = cache.getRecords(window, optimizedSWF);
        }

        if (records == null || records.isEmpty()) {
            return new SDataChunk(-1, ByteBuffer.allocate(0), true);
        }

        int remaining = records.size() - offset;
        SDataChunk dataChunk;
        int recordSize = records.get(0).length;
        int MAX_RECORD_NUM = Args.MAX_CHUNK_SIZE / recordSize;
        if(remaining <= MAX_RECORD_NUM){
            ByteBuffer buffer = ByteBuffer.allocate(remaining * recordSize);
            for(int i = offset; i < records.size(); i++){
                buffer.put(records.get(i));
            }
            buffer.flip();
            dataChunk = new SDataChunk(-1, buffer, true);
        }
        else{
            ByteBuffer buffer = ByteBuffer.allocate(MAX_RECORD_NUM * recordSize);
            for(int i = offset; i < offset + MAX_RECORD_NUM; i++){
                buffer.put(records.get(i));
            }
            buffer.flip();
            dataChunk = new SDataChunk(-1, buffer, false);

            offset += MAX_RECORD_NUM;
        }
        return dataChunk;
    }
}
