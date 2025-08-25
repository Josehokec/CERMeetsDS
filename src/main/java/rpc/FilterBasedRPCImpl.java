package rpc;

import compute.Args;
import filter.BasicBF;
import filter.OptimizedSWF;
import filter.SWF;
import filter.WindowWiseBF;
import org.apache.thrift.TException;
import parser.EqualDependentPredicate;
import rpc.iface.FilterBasedRPC;
import rpc.iface.FilteredResult;
import rpc.iface.SameDataChunk;
import store.EventCache;
import store.FullScan;

import java.nio.ByteBuffer;
import java.util.*;

public class FilterBasedRPCImpl implements FilterBasedRPC.Iface{
    private EventCache cache;
    private List<String> hasFilteredVarNames;
    private OptimizedSWF optimizedSWF;
    private SWF swf;
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
        swf = null;

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
    public Map<String, ByteBuffer> getBF4EQJoin(String varName, long window, Map<String, List<String>> dpStrMap,
                                                Map<String, Integer> keyNumMap, ByteBuffer buff) throws TException {
        if(Args.isOptimizedSwf){
            optimizedSWF = OptimizedSWF.deserialize(buff);
            for(int i = 0; i < hasFilteredVarNames.size() - 1; i++){
                cache.simpleFilter(hasFilteredVarNames.get(i), window, optimizedSWF);
            }
        }else{
            swf = SWF.deserialize(buff);
            for(int i = 0; i < hasFilteredVarNames.size() - 1; i++){
                cache.simpleFilter(hasFilteredVarNames.get(i), window, swf);
            }
        }

        int dpMapSize = dpStrMap.size();
        Map<String, ByteBuffer> bfMap = new HashMap<>(dpMapSize << 1);
        for(String preVarName : dpStrMap.keySet()) {
            List<String> dpStrList = dpStrMap.get(preVarName);
            List<EqualDependentPredicate> dpList = new ArrayList<>(dpStrList.size());
            for (String dpStr : dpStrList) {
                dpList.add(new EqualDependentPredicate(dpStr));
            }
            ByteBuffer buffer = cache.generateBloomFilter(preVarName, keyNumMap.get(preVarName), window, dpList);
            // System.out.println("generate bf size: " + buffer.capacity() + " bytes");//debug
            bfMap.put(preVarName, buffer);
        }
        return bfMap;
    }

    @Override
    public FilteredResult windowFilter(String varName, long window, int headTailMarker, ByteBuffer buff) throws TException {
        ByteBuffer ans;
        if(Args.isOptimizedSwf){
            optimizedSWF = OptimizedSWF.deserialize(buff);
            ans = cache.updatePointers(varName, window, headTailMarker, optimizedSWF);
        }else{
            swf = SWF.deserialize(buff);
            ans = cache.updatePointers(varName, window, headTailMarker, swf);
        }

        hasFilteredVarNames.add(varName);
        return new FilteredResult(cache.getEventNum(varName), ans);
    }

    @Override
    public FilteredResult eqJoinFilter(String varName, long window, int headTailMarker, Map<String, Boolean> previousOrNext,
                                       Map<String, List<String>> dpStrMap, Map<String, ByteBuffer> bfBufferMap) throws TException {
        long startTime = System.currentTimeMillis();
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

        ByteBuffer buffer;
        if(Args.isOptimizedSwf){
            buffer = cache.updatePointers(varName, window, headTailMarker, optimizedSWF, previousOrNext, bfMap, dpMap);
        }else{
            buffer = cache.updatePointers(varName, window, headTailMarker, swf, previousOrNext, bfMap, dpMap);
        }

        hasFilteredVarNames.add(varName);

        long endTime = System.currentTimeMillis();
        System.out.println("varName: " + varName + " filter cost: " + (endTime - startTime) + "ms");
        return new FilteredResult(cache.getEventNum(varName), buffer);
    }

    @Override
    public SameDataChunk getAllFilteredEvents(long window, ByteBuffer updatedSWF) throws TException {
        if(offset == 0){
            if(Args.isOptimizedSwf){
                optimizedSWF = OptimizedSWF.deserialize(updatedSWF);
                records = cache.getRecords(window, optimizedSWF);
            }
            else{
                swf = SWF.deserialize(updatedSWF);
                records = cache.getRecords(window, swf);
            }
        }

        int remaining = records.size() - offset;
        SameDataChunk dataChunk;
        int recordSize = records.get(0).length;
        int MAX_RECORD_NUM = Args.MAX_CHUNK_SIZE / recordSize;
        if(remaining <= MAX_RECORD_NUM){
            ByteBuffer buffer = ByteBuffer.allocate(remaining * recordSize);
            for(int i = offset; i < records.size(); i++){
                buffer.put(records.get(i));
            }
            buffer.flip();
            dataChunk = new SameDataChunk(-1, buffer, true);
        }
        else{
            ByteBuffer buffer = ByteBuffer.allocate(MAX_RECORD_NUM * recordSize);
            for(int i = offset; i < offset + MAX_RECORD_NUM; i++){
                buffer.put(records.get(i));
            }
            buffer.flip();
            dataChunk = new SameDataChunk(-1, buffer, false);

            offset += MAX_RECORD_NUM;
        }
        return dataChunk;
    }
}
