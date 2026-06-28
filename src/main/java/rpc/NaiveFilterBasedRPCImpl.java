package rpc;

import compute.Args;
import org.apache.thrift.TException;
import parser.EqualDependentPredicate;
import rpc.iface.BasicFilterBasedRPC;
import rpc.iface.SDataChunk;
import store.EventCache;
import store.FullScan;
import utils.ReplayIntervals;

import java.nio.ByteBuffer;
import java.util.*;

public class NaiveFilterBasedRPCImpl implements BasicFilterBasedRPC.Iface {
    public static boolean enableInOrderInsert = false;
    private EventCache cache;
    private List<String> hasFilteredVarNames;
    private ReplayIntervals localIntervals;
    public static int queryId = 0;
    public List<byte[]> records;
    public int offset;

    @Override
    public Map<String, Integer> initial(String tableName, Map<String, List<String>> ipStrMap) throws TException {
        System.out.println(queryId + "-th query arrives...");
        enableInOrderInsert = true;
        queryId++;
        records = null;
        localIntervals = null;
        offset = 0;

        hasFilteredVarNames = new ArrayList<>(8);
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
    public Map<String, Map<Long, Set<String>>> getHashTable4EQJoin(String varName, long window, Map<String, List<String>> dpStrMap, ByteBuffer intervals) throws TException {
        ReplayIntervals ris = ReplayIntervals.deserialize(intervals);
        for(int i = 0; i < hasFilteredVarNames.size() - 1; i++){
            cache.simpleFilter(hasFilteredVarNames.get(i), ris);
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
        localIntervals = ris;
        return ans;
    }

    @Override
    public ByteBuffer windowFilter(String varName, long window, int headTailMarker, ByteBuffer intervals) throws TException {
        ReplayIntervals replayIntervals = ReplayIntervals.deserialize(intervals);
        ByteBuffer intervalBuff = cache.updatePointers2(varName, window, headTailMarker, replayIntervals);
        hasFilteredVarNames.add(varName);
        return intervalBuff;
    }

    @Override
    public ByteBuffer eqJoinFilter(String varName, long window, int headTailMarker, Map<String, Boolean> previousOrNext,
                                   Map<String, List<String>> dpStrMap, Map<String, Map<Long, Set<String>>> pairs) throws TException {

        long startTime = System.currentTimeMillis();
        int previousVarNum = previousOrNext.size();

        // replay intervals
        Map<String, List<EqualDependentPredicate>> dpMap = new HashMap<>(previousVarNum << 1);

        for(String previousVariableName : pairs.keySet()){
            List<EqualDependentPredicate> dps = new ArrayList<>();
            List<String> dpStrList = dpStrMap.get(previousVariableName);
            for(String dpStr : dpStrList){
                dps.add(new EqualDependentPredicate(dpStr));
            }
            dpMap.put(previousVariableName, dps);
        }

        ByteBuffer buffer = cache.updatePointers(varName, window, headTailMarker, localIntervals, previousOrNext, pairs, dpMap);

        hasFilteredVarNames.add(varName);

        long endTime = System.currentTimeMillis();
        System.out.println("varName: " + varName + " filter cost: " + (endTime - startTime) + "ms");
        return buffer;
    }

    @Override
    public SDataChunk getAllFilteredEvents(long window, ByteBuffer intervals) throws TException {
        if(offset == 0){
            // only called once
            ReplayIntervals replayIntervals = ReplayIntervals.deserialize(intervals);
            records = cache.getRecords(replayIntervals);
        }

        int remaining = records.size() - offset;

        SDataChunk dataChunk;

        int recordSize = records.get(0).length;
        int MAX_RECORD_NUM = Args.MAX_CHUNK_SIZE / recordSize;
        if(remaining <= MAX_RECORD_NUM) {
            ByteBuffer buffer = ByteBuffer.allocate(remaining * recordSize);
            for (int i = offset; i < records.size(); i++) {
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
