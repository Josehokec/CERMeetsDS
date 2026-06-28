package compute;

import engine.SelectionStrategy;
import event.*;
import filter.OptimizedSWF;
import filter.SWF;
import filter.UpdatedMarkers;
import filter.WindowWiseBF;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import parser.EqualDependentPredicate;
import parser.IndependentPredicate;
import parser.PredicateReplayIntervalIndex;
import parser.QueryParse;
import plan.*;
import request.ReadQueries;
import rpc.iface.ExtendDataChunk;
import rpc.iface.ExtendFilterBasedRPC;
import rpc.iface.ExtendFilteredResult;
import rpc.iface.FilteredResult;
import store.EventSchema;
import store.FullScan;
import utils.Pair;
import utils.ReplayIntervals;
import utils.SortByTs;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class RunExtendOurs {
    static double networkBandwidth = 0.1; // Byte/ns
    static PredicateReplayIntervalIndex predicateReplayIntervalIndex;

    interface MeasuredTask {
        long getElapsedTime();
    }

    static byte[] orByteArrays(byte[] array1, byte[] array2) {
        int length = Math.min(array1.length, array2.length);
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = (byte) (array1[i] | array2[i]);
        }
        return result;
    }

    static void mergeBloomFilterBuffers(Map<String, byte[]> mergedBFBufferMap, Map<String, ByteBuffer> bfBufferMap) {
        for(String key : bfBufferMap.keySet()){
            ByteBuffer bfBuffer = bfBufferMap.get(key).duplicate();
            int size = bfBuffer.remaining();
            byte[] byteArray = new byte[size];
            bfBuffer.get(byteArray);
            if(mergedBFBufferMap.containsKey(key)){
                byte[] mergedBF = orByteArrays(byteArray, mergedBFBufferMap.get(key));
                mergedBFBufferMap.put(key, mergedBF);
            }else{
                mergedBFBufferMap.put(key, byteArray);
            }
        }
    }

    static void mergeCardinality(Map<String, Integer> totalMap, Map<String, Integer> addMap) {
        for(Map.Entry<String, Integer> entry : addMap.entrySet()){
            String key = entry.getKey();
            int value = entry.getValue();
            totalMap.put(key, totalMap.getOrDefault(key, 0) + value);
        }
    }

    static ByteBuffer duplicateBuffer(ByteBuffer buffer) {
        return buffer == null ? null : buffer.duplicate();
    }

    static Map<String, ByteBuffer> duplicateBufferMap(Map<String, ByteBuffer> bufferMap) {
        Map<String, ByteBuffer> duplicated = new HashMap<>(bufferMap.size() << 1);
        for(Map.Entry<String, ByteBuffer> entry : bufferMap.entrySet()){
            duplicated.put(entry.getKey(), duplicateBuffer(entry.getValue()));
        }
        return duplicated;
    }

    static long maxElapsedTime(List<? extends MeasuredTask> tasks) {
        long max = 0;
        if(tasks == null){
            return max;
        }
        for(MeasuredTask task : tasks){
            max = Math.max(max, task.getElapsedTime());
        }
        return max;
    }

    static long sumElapsedTime(List<? extends MeasuredTask> tasks) {
        long sum = 0;
        if(tasks == null){
            return sum;
        }
        for(MeasuredTask task : tasks){
            sum += task.getElapsedTime();
        }
        return sum;
    }

    static void logParallelPhase(String phaseName, long wallTime, MeasuredTask cacheTask,
                                 List<? extends MeasuredTask> storageTasks) {
        long cacheTime = cacheTask == null ? 0 : cacheTask.getElapsedTime();
        long storageMaxTime = maxElapsedTime(storageTasks);
        long storageTotalTime = sumElapsedTime(storageTasks);
        System.out.println("phase " + phaseName + " cost: wall=" + wallTime
                + "ms, cache=" + cacheTime
                + "ms, storageMax=" + storageMaxTime
                + "ms, storageTotal=" + storageTotalTime + "ms");
    }

    static class InitialThread extends Thread implements MeasuredTask {
        private final ExtendFilterBasedRPC.Client client;
        private final String tableName;
        // ip: independent predicate
        private final Map<String, List<String>> ipMap;
        private final ByteBuffer intervals;
        private Map<String, Integer> varEventNumMap;
        private long communicationCost;
        private long elapsedTime;

        public InitialThread(ExtendFilterBasedRPC.Client client, String tableName, Map<String, List<String>> ipMap){
            this(client, tableName, ipMap, null);
        }

        public InitialThread(ExtendFilterBasedRPC.Client client, String tableName, Map<String, List<String>> ipMap,
                             ByteBuffer intervals){
            this.client = client;
            this.tableName = tableName;
            this.ipMap = ipMap;
            this.intervals = intervals == null ? null : intervals.duplicate();
            communicationCost = 0;
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public Map<String, Integer> getVarEventNumMap(){
            return varEventNumMap;
        }

        @Override
        public long getElapsedTime() {
            return elapsedTime;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try {
                communicationCost += tableName.length() + ipMap.toString().length();
                if(intervals == null){
                    varEventNumMap = client.initial(tableName, ipMap);
                }else{
                    varEventNumMap = client.extendInitial(tableName, ipMap, intervals.duplicate());
                    communicationCost += intervals.capacity();
                }
                communicationCost += varEventNumMap.toString().length();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class ComputeCacheScanThread extends Thread implements MeasuredTask {
        private final ComputeNodeCache computeCacheStore;
        private final Map<String, List<String>> ipMap;
        private final ReplayIntervals replayIntervals;
        private final int cacheThreadNum;
        private ComputeNodeCache computeCache;
        private long scanCost;
        private RuntimeException error;

        public ComputeCacheScanThread(ComputeNodeCache computeCacheStore, Map<String, List<String>> ipMap,
                                      int cacheThreadNum) {
            this(computeCacheStore, ipMap, null, cacheThreadNum);
        }

        public ComputeCacheScanThread(ComputeNodeCache computeCacheStore, Map<String, List<String>> ipMap,
                                      ReplayIntervals replayIntervals, int cacheThreadNum) {
            this.computeCacheStore = computeCacheStore;
            this.ipMap = ipMap;
            this.replayIntervals = replayIntervals;
            this.cacheThreadNum = cacheThreadNum;
            this.scanCost = 0;
        }

        public ComputeNodeCache getComputeCache() {
            return computeCache;
        }

        public long getScanCost() {
            return scanCost;
        }

        @Override
        public long getElapsedTime() {
            return scanCost;
        }

        public RuntimeException getError() {
            return error;
        }

        @Override
        public void run() {
            if(computeCacheStore == null){
                return;
            }

            long startTime = System.currentTimeMillis();
            try{
                if(replayIntervals == null){
                    computeCache = computeCacheStore.scanInParallel(ipMap, cacheThreadNum);
                }else{
                    computeCache = computeCacheStore.scanInParallel(ipMap, replayIntervals, cacheThreadNum);
                }
            }catch (RuntimeException e){
                error = e;
            }finally {
                long endTime = System.currentTimeMillis();
                scanCost = endTime - startTime;
            }
        }
    }

    static class CacheGenerateIntervalThread extends Thread implements MeasuredTask {
        private final ComputeNodeCache computeCache;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private ReplayIntervals ri;
        private RuntimeException error;
        private long elapsedTime;

        public CacheGenerateIntervalThread(ComputeNodeCache computeCache, String varName, long window, int headTailMarker) {
            this.computeCache = computeCache;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
        }

        public ReplayIntervals getReplayIntervals() {
            return ri;
        }

        public RuntimeException getError() {
            return error;
        }

        @Override
        public long getElapsedTime() {
            return elapsedTime;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try {
                ByteBuffer buffer = computeCache.getInitialIntervals(varName, window, headTailMarker);
                ri = ReplayIntervals.deserialize(buffer);
            } catch (RuntimeException e) {
                error = e;
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class GenerateIntervalThread extends Thread implements MeasuredTask {
        private final ExtendFilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private ReplayIntervals ri;
        private long communicationCost;
        private long elapsedTime;

        public GenerateIntervalThread(ExtendFilterBasedRPC.Client client, String varName, long window, int headTailMarker){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            communicationCost = 0;
        }

        public ReplayIntervals getReplayIntervals(){
            return ri;
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        @Override
        public long getElapsedTime() {
            return elapsedTime;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try {
                ByteBuffer buffer = client.getInitialIntervals(varName, window, headTailMarker);
                communicationCost += varName.length() + 8 + 4;
                communicationCost += buffer.capacity();
                ri = ReplayIntervals.deserialize(buffer);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class CacheWindowFilterThread extends Thread implements MeasuredTask {
        private final ComputeNodeCache computeCache;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final ByteBuffer swfBuffer;
        private FilteredResult filteredResult;
        private RuntimeException error;
        private long elapsedTime;

        public CacheWindowFilterThread(ComputeNodeCache computeCache, String varName, long window,
                                       int headTailMarker, ByteBuffer swfBuffer) {
            this.computeCache = computeCache;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.swfBuffer = duplicateBuffer(swfBuffer);
        }

        public FilteredResult getFilteredResult() {
            return filteredResult;
        }

        public RuntimeException getError() {
            return error;
        }

        @Override
        public long getElapsedTime() {
            return elapsedTime;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try {
                filteredResult = computeCache.windowFilter(varName, window, headTailMarker, swfBuffer);
            } catch (RuntimeException e) {
                error = e;
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class WindowFilterThread extends Thread implements MeasuredTask {
        private final ExtendFilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final ByteBuffer swfBuffer;

        private UpdatedMarkers updateMarkers;
        private SWF updatedSWF;
        private int filteredNum;
        private long communicationCost;
        private long elapsedTime;

        public WindowFilterThread(ExtendFilterBasedRPC.Client client, String varName, long window, int headTailMarker, ByteBuffer swfBuffer){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.swfBuffer = duplicateBuffer(swfBuffer);
            communicationCost = 0;
        }

        public int getFilteredNum(){
            return filteredNum;
        }

        public UpdatedMarkers getUpdateMarkers(){
            return updateMarkers;
        }

        public SWF getUpdatedSWF(){
            return updatedSWF;
        }

        public long getCommunicationCost(){
            return communicationCost;
        }

        @Override
        public long getElapsedTime() {
            return elapsedTime;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try{
                ExtendFilteredResult res  = client.windowFilter(varName, window, headTailMarker, swfBuffer);
                communicationCost +=  varName.length() + 8 + 4 + swfBuffer.capacity();
                communicationCost += res.updatedSWF.capacity();

                if(Args.isOptimizedSwf){
                    updateMarkers = UpdatedMarkers.deserialize(res.updatedSWF);
                }else{
                    updatedSWF = SWF.deserialize(res.updatedSWF);
                }
                filteredNum = res.filteredNum;
            }catch (Exception e){
                e.printStackTrace();
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class CacheRequestBloomFilterThread extends Thread implements MeasuredTask {
        private final ComputeNodeCache computeCache;
        private final String varName;
        private final long window;
        private final Map<String, List<String>> sendDPMap;
        private final Map<String, Integer> eventNumMap;
        private final ByteBuffer swf;
        private Map<String, ByteBuffer> bfBufferMap;
        private RuntimeException error;
        private long elapsedTime;

        public CacheRequestBloomFilterThread(ComputeNodeCache computeCache, String varName, long window,
                                             Map<String, List<String>> sendDPMap, Map<String, Integer> eventNumMap,
                                             ByteBuffer swf) {
            this.computeCache = computeCache;
            this.varName = varName;
            this.window = window;
            this.sendDPMap = sendDPMap;
            this.eventNumMap = eventNumMap;
            this.swf = duplicateBuffer(swf);
        }

        public Map<String, ByteBuffer> getBfBufferMap() {
            return bfBufferMap;
        }

        public RuntimeException getError() {
            return error;
        }

        @Override
        public long getElapsedTime() {
            return elapsedTime;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try {
                bfBufferMap = computeCache.getBF4EQJoin(varName, window, sendDPMap, eventNumMap, swf);
            } catch (RuntimeException e) {
                error = e;
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class RequestBloomFilterThread extends Thread implements MeasuredTask {
        private final ExtendFilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final Map<String, List<String>> sendDPMap;
        private final Map<String, Integer> eventNumMap;
        private final ByteBuffer swf;
        private Map<String, ByteBuffer> bfBufferMap;
        private long communicationCost;
        private long elapsedTime;

        public Map<String, ByteBuffer> getBfBufferMap(){
            return bfBufferMap;
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        @Override
        public long getElapsedTime() {
            return elapsedTime;
        }

        public RequestBloomFilterThread(ExtendFilterBasedRPC.Client client, String varName, long window,
                                        Map<String, List<String>> sendDPMap, Map<String, Integer> eventNumMap, ByteBuffer swf){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.sendDPMap = sendDPMap;
            this.eventNumMap = eventNumMap;
            this.swf = duplicateBuffer(swf);
            this.communicationCost = 0;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try{
                bfBufferMap = client.getBF4EQJoin(varName, window, sendDPMap, eventNumMap, swf);
                communicationCost = varName.length() + 8 + sendDPMap.toString().length() + swf.capacity();
                for(ByteBuffer b : bfBufferMap.values()){
                    communicationCost += b.remaining();
                }
            }catch (Exception e){
                e.printStackTrace();
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class CacheEqJoinFilterThread extends Thread implements MeasuredTask {
        private final ComputeNodeCache computeCache;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final Map<String, Boolean> previousOrNext;
        private final Map<String, List<String>> sendDPMap;
        private final Map<String, ByteBuffer> bfBufferMap;
        private FilteredResult filteredResult;
        private RuntimeException error;
        private long elapsedTime;

        public CacheEqJoinFilterThread(ComputeNodeCache computeCache, String varName, long window, int headTailMarker,
                                       Map<String, Boolean> previousOrNext, Map<String, List<String>> sendDPMap,
                                       Map<String, ByteBuffer> bfBufferMap) {
            this.computeCache = computeCache;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.previousOrNext = previousOrNext;
            this.sendDPMap = sendDPMap;
            this.bfBufferMap = duplicateBufferMap(bfBufferMap);
        }

        public FilteredResult getFilteredResult() {
            return filteredResult;
        }

        public RuntimeException getError() {
            return error;
        }

        @Override
        public long getElapsedTime() {
            return elapsedTime;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try {
                filteredResult = computeCache.eqJoinFilter(varName, window, headTailMarker, previousOrNext, sendDPMap, bfBufferMap);
            } catch (RuntimeException e) {
                error = e;
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class EqJoinFilterThread extends Thread implements MeasuredTask {
        private final ExtendFilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final Map<String, Boolean> previousOrNext;
        private final Map<String, List<String>> sendDPMap;
        private final Map<String, ByteBuffer> bfBufferMap;
        private UpdatedMarkers updatedMarkers;
        private SWF updatedSWF;
        private long communicationCost;
        private long elapsedTime;

        private int filteredNum;

        public EqJoinFilterThread(ExtendFilterBasedRPC.Client client, String varName, long window, int headTailMarker,
                                  Map<String, Boolean> previousOrNext, Map<String, List<String>> sendDPMap, Map<String, ByteBuffer> bfBufferMap){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.previousOrNext = previousOrNext;
            this.sendDPMap = sendDPMap;
            this.bfBufferMap = duplicateBufferMap(bfBufferMap);

            communicationCost = varName.length() + 8 + 4 + sendDPMap.toString().length() + previousOrNext.toString().length();
            for (ByteBuffer b : bfBufferMap.values()){
                communicationCost += b.remaining();
            }
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public UpdatedMarkers getUpdatedMarkers(){
            return updatedMarkers;
        }

        public SWF getUpdatedSWF(){
            return updatedSWF;
        }

        public int getFilteredNum() {
            return filteredNum;
        }

        @Override
        public long getElapsedTime() {
            return elapsedTime;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try{
                ExtendFilteredResult ans  = client.eqJoinFilter(varName, window, headTailMarker, previousOrNext, sendDPMap, bfBufferMap);
                communicationCost += ans.updatedSWF.capacity() + 4;
                filteredNum = ans.filteredNum;
                if(Args.isOptimizedSwf){
                    updatedMarkers = UpdatedMarkers.deserialize(ans.updatedSWF);
                }else{
                    updatedSWF = SWF.deserialize(ans.updatedSWF);
                }
            }catch (Exception e){
                e.printStackTrace();
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class CachePullEventThread extends Thread implements MeasuredTask {
        private final ComputeNodeCache computeCache;
        private final long window;
        private final ByteBuffer swfBuffer;
        private List<byte[]> events;
        private RuntimeException error;
        private long elapsedTime;

        public CachePullEventThread(ComputeNodeCache computeCache, long window, ByteBuffer swfBuffer) {
            this.computeCache = computeCache;
            this.window = window;
            this.swfBuffer = duplicateBuffer(swfBuffer);
        }

        public List<byte[]> getAllEvents() {
            return events;
        }

        public RuntimeException getError() {
            return error;
        }

        @Override
        public long getElapsedTime() {
            return elapsedTime;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try {
                events = computeCache.getAllFilteredEvents(window, swfBuffer);
            } catch (RuntimeException e) {
                error = e;
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class PullEventThread extends Thread implements MeasuredTask {
        private final ExtendFilterBasedRPC.Client client;
        private final long window;
        private final ByteBuffer swfBuffer;
        private final int recordLen;

        private List<byte[]> events;
        private long communicationCost;
        private long elapsedTime;

        public PullEventThread(ExtendFilterBasedRPC.Client client, long window, ByteBuffer swfBuffer, int recordLen){
            this.client = client;
            this.window = window;
            this.swfBuffer = duplicateBuffer(swfBuffer);
            this.recordLen = recordLen;

            communicationCost = 12 + swfBuffer.capacity();
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public List<byte[]> getAllEvents(){
            return events;
        }

        @Override
        public long getElapsedTime() {
            return elapsedTime;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try{
                boolean isLastChunk = true;
                do {
                    ExtendDataChunk chunk;
                    if(isLastChunk){
                        chunk  = client.getAllFilteredEvents(window, swfBuffer);
                    }else{
                        chunk  = client.getAllFilteredEvents(window, null);
                    }

                    int recordNum = chunk.data.remaining() / recordLen;
                    if(events == null){
                        events = new ArrayList<>(recordNum * 3 /2);
                    }

                    for(int i = 0; i < recordNum; ++i){
                        byte[] record = new byte[recordLen];
                        chunk.data.get(record);
                        events.add(record);
                    }
                    isLastChunk = chunk.isLastChunk;
                    communicationCost += chunk.data.capacity();
                }while(!isLastChunk);

            }catch (Exception e){
                e.printStackTrace();
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    public static List<byte[]> communicate(String sql, List<ExtendFilterBasedRPC.Client> clients,
                                           ComputeNodeCache computeCacheStore, int cacheThreadNum){
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        Map<String, List<String>> ipMap = query.getIpStringMap();
        Map<String, List<String>> equalDPMap = query.getEqualDpMap();
        long window = query.getWindow();
        int nodeNum = clients.size();
        EventSchema schema = EventSchema.getEventSchema(tableName);

        long transmissionSize = 0;
        // key: variable name, value: number of events, number of slices
        Map<String, Pair<Integer, Long>> filteredEventNum = new HashMap<>();

        if(predicateReplayIntervalIndex == null){
            predicateReplayIntervalIndex = new PredicateReplayIntervalIndex();
        }

        Set<String> hitCacheVarNames = new HashSet<>(16);
        Set<String> exactMatchVarNames = new HashSet<>(16);
        ReplayIntervals cachedIntervals = null;

        for(Map.Entry<String, List<String>> entry : ipMap.entrySet()){
            String varName = entry.getKey();
            List<String> ipStrList = entry.getValue();
            List<IndependentPredicate> ipList = new ArrayList<>(ipStrList.size() + 2);
            for(String ipStr : ipStrList){
                ipList.add(new IndependentPredicate(ipStr));
            }
            ipList.add(new IndependentPredicate("x.w<=" + window));
            ipList.add(new IndependentPredicate("x.m=" + query.headTailMarker(varName)));
            PredicateReplayIntervalIndex.QueryResult queryResult = predicateReplayIntervalIndex.query(ipList);
            if(queryResult != null){
                hitCacheVarNames.add(varName);
                if(queryResult.exactMatch == 1){
                    exactMatchVarNames.add(varName);
                }
                if(cachedIntervals == null){
                    cachedIntervals = queryResult.intervals;
                }else{
                    cachedIntervals.intersect(queryResult.intervals);
                }
            }
        }
        System.out.println("hit predicate interval cache? " + (hitCacheVarNames.isEmpty() ? "no" : "yes"));

        if(cachedIntervals != null){
            int intervalSize = cachedIntervals.getIntervals().size();
            if(intervalSize == 0){
                cachedIntervals = null;
                hitCacheVarNames.clear();
                exactMatchVarNames.clear();
            }else{
                long averageLen = cachedIntervals.getTimeLength() / intervalSize;
                System.out.println("cached intervals: " + intervalSize + ", average length: "
                        + averageLen + ", window size: " + window);
                if(averageLen > 2.1 * window){
                    System.out.println("remove predicate interval cache result because of low quality");
                    cachedIntervals = null;
                    hitCacheVarNames.clear();
                    exactMatchVarNames.clear();
                }
            }
        }

        // step 1: initial (process independent predicates and return cardinality)
        long initialStartTime = System.currentTimeMillis();
        ComputeCacheScanThread computeCacheScanThread = null;
        ByteBuffer initialIntervals = cachedIntervals == null ? null : cachedIntervals.serialize();
        ReplayIntervals scanIntervals = initialIntervals == null ?
                null : ReplayIntervals.deserialize(initialIntervals.duplicate());
        if(computeCacheStore != null){
            computeCacheScanThread = new ComputeCacheScanThread(computeCacheStore, ipMap, scanIntervals, cacheThreadNum);
            computeCacheScanThread.start();
        }

        List<InitialThread> initialThreads = new ArrayList<>(nodeNum);
        for(ExtendFilterBasedRPC.Client client : clients){
            InitialThread initialThread = new InitialThread(client, tableName, ipMap, initialIntervals);
            initialThread.start();
            initialThreads.add(initialThread);
        }
        for (InitialThread t : initialThreads) {
            try{ t.join(); }catch (Exception e){ e.printStackTrace(); }
        }
        ComputeNodeCache computeCache = null;
        if(computeCacheScanThread != null){
            try{
                computeCacheScanThread.join();
            }catch (Exception e){
                e.printStackTrace();
            }
            if(computeCacheScanThread.getError() != null){
                throw computeCacheScanThread.getError();
            }
            computeCache = computeCacheScanThread.getComputeCache();
            System.out.println("compute cache scan cost: " + computeCacheScanThread.getScanCost()
                    + "ms, threads=" + cacheThreadNum);
        }
        logParallelPhase("initial", System.currentTimeMillis() - initialStartTime, computeCacheScanThread, initialThreads);

        // aggregate number of events
        Map<String, Integer> varEventNumMap = new HashMap<>(ipMap.size() << 1);
        if(computeCache != null){
            Map<String, Integer> cacheCardinality = computeCache.getCardinality();
            mergeCardinality(varEventNumMap, cacheCardinality);
            // System.out.println("compute cache cardinality: " + cacheCardinality);
        }
        for(InitialThread t : initialThreads){
            transmissionSize += t.getCommunicationCost();
            Map<String, Integer> map = t.getVarEventNumMap();
            mergeCardinality(varEventNumMap, map);
        }
        System.out.println("varEventNumMap: " + varEventNumMap);


        OptimizedSWF optimizedSWF = null;
        UpdatedMarkers updatedMarkers = null;

        SWF swf = null;

        Plan plan  = GeneratedPlan.filterPlan(varEventNumMap, query.getHeadVarName(), query.getTailVarName(), query.getDpMap());

        List<String> steps = plan.getSteps();
        Set<String> hasProcessedVarName = new HashSet<>();
        long slicedBits = -1;

        // if you can get a good parameter, we suggest to remove cost model to avoid negative impact
        // AdaptiveCostModel
        CostEstimator costEstimator = new AlwaysOnCostModel(nodeNum, query, varEventNumMap, schema);

        for(String varName : steps){
            if(hasProcessedVarName.isEmpty()){
                // initialization...
                long intervalStartTime = System.currentTimeMillis();
                ReplayIntervals replayIntervals = null;
                if(exactMatchVarNames.contains(varName)){
                    replayIntervals = cachedIntervals;
                    System.out.println("reuse predicate replay intervals for variable " + varName);
                }else{
                    CacheGenerateIntervalThread cacheGenerateIntervalThread = null;
                    if(computeCache != null){
                        cacheGenerateIntervalThread = new CacheGenerateIntervalThread(computeCache, varName, window, query.headTailMarker(varName));
                        cacheGenerateIntervalThread.start();
                    }

                    List<GenerateIntervalThread> generateIntervalThreads = new ArrayList<>(nodeNum);
                    for(ExtendFilterBasedRPC.Client client : clients){
                        GenerateIntervalThread thread = new GenerateIntervalThread(client, varName, window, query.headTailMarker(varName));
                        thread.start();
                        generateIntervalThreads.add(thread);
                    }
                    for (GenerateIntervalThread t : generateIntervalThreads) {
                        try{ t.join(); }catch (Exception e){ e.printStackTrace(); }
                    }
                    if(cacheGenerateIntervalThread != null){
                        try{
                            cacheGenerateIntervalThread.join();
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                        if(cacheGenerateIntervalThread.getError() != null){
                            throw cacheGenerateIntervalThread.getError();
                        }
                        replayIntervals = cacheGenerateIntervalThread.getReplayIntervals();
                    }
                    logParallelPhase("initial-interval-" + varName,
                            System.currentTimeMillis() - intervalStartTime,
                            cacheGenerateIntervalThread,
                            generateIntervalThreads);

                    // union
                    for(GenerateIntervalThread t : generateIntervalThreads){
                        transmissionSize += t.getCommunicationCost();
                        if(replayIntervals == null){
                            replayIntervals = t.getReplayIntervals();
                        }else{
                            ReplayIntervals ri = t.getReplayIntervals();
                            replayIntervals.union(ri);
                        }
                    }

                    List<String> ipStrList = ipMap.get(varName);
                    List<IndependentPredicate> ipList = new ArrayList<>(ipStrList.size() + 2);
                    for(String ipStr : ipStrList){
                        ipList.add(new IndependentPredicate(ipStr));
                    }
                    ipList.add(new IndependentPredicate("x.w<=" + window));
                    ipList.add(new IndependentPredicate("x.m=" + query.headTailMarker(varName)));
                    predicateReplayIntervalIndex.putCondition(ipList, replayIntervals);
                }
                int keyNum = replayIntervals.getKeyNumber(window);
                List<ReplayIntervals.TimeInterval> timeIntervals = replayIntervals.getIntervals();
                if(Args.isOptimizedSwf){
                    optimizedSWF = new OptimizedSWF.Builder(keyNum).build();
                    for(ReplayIntervals.TimeInterval interval : timeIntervals) {
                        optimizedSWF.insert(interval.getStartTime(), interval.getEndTime(), window);
                    }
                    slicedBits = optimizedSWF.getSliceNum();
                }
                else{
                    swf = new SWF.Builder(keyNum).build();
                    for(ReplayIntervals.TimeInterval interval : timeIntervals) {
                        swf.insert(interval.getStartTime(), interval.getEndTime(), window);
                    }
                    slicedBits = swf.getSliceNum();
                }

                hasProcessedVarName.add(varName);
                filteredEventNum.put(varName, new Pair<>(varEventNumMap.get(varName), slicedBits));
            }
            else{
                Map<String, List<String>> sendDPMap = new HashMap<>(8);
                Map<String, Integer> eventNumMap = new HashMap<>(8);
                Map<String, Boolean> previousOrNext = new HashMap<>(8);

                // used for cost model
                int bfSize = 0;
                double keyNum = 0;
                boolean hasDP = false;
                // here we check whether query has equal dependent predicates, and estimate key number
                for(String hasVisitVarName : hasProcessedVarName){
                    String key = hasVisitVarName.compareTo(varName) < 0 ? (hasVisitVarName + "-" + varName) : (varName + "-" + hasVisitVarName);
                    if(equalDPMap.containsKey(key)){
                        hasDP = true;
                        sendDPMap.put(hasVisitVarName, equalDPMap.get(key));
                        // then we need to estimate number of keys
                        Pair<Integer, Long> p = filteredEventNum.get(hasVisitVarName);
                        int filterKeyNum = Args.isOptimizedSwf ? optimizedSWF.getKeyNum() : swf.getKeyNum();
                        // one window
                        double estimateEventNum = (p.getKey() * slicedBits + 0.0) / p.getValue();
                        keyNum += estimateEventNum;
                        bfSize += WindowWiseBF.getEstimatedBitSize(estimateEventNum);
                        int estimateKeyNum;
                        if(equalDPMap.get(key).size() == 1){
                            EqualDependentPredicate edp = new EqualDependentPredicate(equalDPMap.get(key).get(0));
                            estimateKeyNum = Estimator.calKeyNum(tableName, edp.getAttrName(hasVisitVarName), estimateEventNum, filterKeyNum);
                        }else{
                            System.out.println("we directly use number of events as number of keys");
                            estimateKeyNum = (int) estimateEventNum;
                        }
                        // here we need to modify
                        eventNumMap.put(hasVisitVarName, estimateKeyNum);
                        previousOrNext.put(hasVisitVarName, query.compareSequence(hasVisitVarName, varName));
                    }
                }

                int headTailMarker = query.headTailMarker(varName);
                ByteBuffer swfBuffer = Args.isOptimizedSwf ? optimizedSWF.serialize() : swf.serialize();

                int updatedEventNum = 0;

                long bitCount = Args.isOptimizedSwf ? optimizedSWF.getSliceNum() : swf.getSliceNum();

                //boolean startRoundTrip = true;
                boolean startRoundTrip = costEstimator.newRoundTrip(bitCount, query.headTailMarker(varName),
                        varName, swfBuffer.capacity(), bfSize, sendDPMap.size(), 8, (int) keyNum, networkBandwidth);

                if(startRoundTrip){
                    if(hasDP){
                        long startTime2 = System.currentTimeMillis();
                        long bloomFilterStartTime = System.currentTimeMillis();
                        Map<String, ByteBuffer> cacheBFBufferMap = null;
                        CacheRequestBloomFilterThread cacheRequestBloomFilterThread = null;
                        if(computeCache != null){
                            cacheRequestBloomFilterThread = new CacheRequestBloomFilterThread(
                                    computeCache, varName, window, sendDPMap, eventNumMap, swfBuffer);
                            cacheRequestBloomFilterThread.start();
                        }

                        List<RequestBloomFilterThread> requestBloomFilterThreads = new ArrayList<>(nodeNum);
                        for (ExtendFilterBasedRPC.Client client : clients) {
                            RequestBloomFilterThread thread = new RequestBloomFilterThread(client, varName, window, sendDPMap, eventNumMap, swfBuffer);
                            thread.start();
                            requestBloomFilterThreads.add(thread);
                        }
                        for (RequestBloomFilterThread t : requestBloomFilterThreads){
                            try{ t.join(); }catch (Exception e){ e.printStackTrace(); }
                        }
                        if(cacheRequestBloomFilterThread != null){
                            try{
                                cacheRequestBloomFilterThread.join();
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            if(cacheRequestBloomFilterThread.getError() != null){
                                throw cacheRequestBloomFilterThread.getError();
                            }
                            cacheBFBufferMap = cacheRequestBloomFilterThread.getBfBufferMap();
                        }
                        logParallelPhase("bloom-filter-" + varName,
                                System.currentTimeMillis() - bloomFilterStartTime,
                                cacheRequestBloomFilterThread,
                                requestBloomFilterThreads);
                        // merge all bloom filters
                        Map<String, byte[]> mergedBFBufferMap = new HashMap<>(8);
                        if(cacheBFBufferMap != null){
                            mergeBloomFilterBuffers(mergedBFBufferMap, cacheBFBufferMap);
                        }
                        for (RequestBloomFilterThread t : requestBloomFilterThreads){
                            transmissionSize += t.getCommunicationCost();
                            Map<String, ByteBuffer> bfBufferMap = t.getBfBufferMap();
                            mergeBloomFilterBuffers(mergedBFBufferMap, bfBufferMap);
                        }
                        //System.out.println("bf transmissionSize: " + transmissionSize);
                        Map<String, ByteBuffer> bfBufferMap = new HashMap<>(8);
                        for(String key : mergedBFBufferMap.keySet()){
                            ByteBuffer bfBuffer = ByteBuffer.wrap(mergedBFBufferMap.get(key));
                            bfBufferMap.put(key, bfBuffer);
                        }

                        // join filtering
                        long eqJoinStartTime = System.currentTimeMillis();
                        FilteredResult cacheFilterResult = null;
                        CacheEqJoinFilterThread cacheEqJoinFilterThread = null;
                        if(computeCache != null){
                            cacheEqJoinFilterThread = new CacheEqJoinFilterThread(
                                    computeCache, varName, window, headTailMarker, previousOrNext, sendDPMap, bfBufferMap);
                            cacheEqJoinFilterThread.start();
                        }

                        List<EqJoinFilterThread> eqJoinFilterThreads = new ArrayList<>(nodeNum);
                        for (ExtendFilterBasedRPC.Client client : clients) {
                            EqJoinFilterThread thread = new EqJoinFilterThread(client, varName, window, headTailMarker, previousOrNext, sendDPMap, bfBufferMap);
                            thread.start();
                            eqJoinFilterThreads.add(thread);
                        }
                        for (EqJoinFilterThread t : eqJoinFilterThreads){
                            try{
                                t.join();
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                        if(cacheEqJoinFilterThread != null){
                            try{
                                cacheEqJoinFilterThread.join();
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            if(cacheEqJoinFilterThread.getError() != null){
                                throw cacheEqJoinFilterThread.getError();
                            }
                            cacheFilterResult = cacheEqJoinFilterThread.getFilteredResult();
                        }
                        logParallelPhase("eq-join-filter-" + varName,
                                System.currentTimeMillis() - eqJoinStartTime,
                                cacheEqJoinFilterThread,
                                eqJoinFilterThreads);
                        if(Args.isOptimizedSwf){
                            updatedMarkers = null;
                            if(cacheFilterResult != null){
                                updatedEventNum += cacheFilterResult.filteredNum;
                                updatedMarkers = UpdatedMarkers.deserialize(cacheFilterResult.updatedSWF.duplicate());
                            }
                            for (EqJoinFilterThread t : eqJoinFilterThreads) {
                                transmissionSize += t.getCommunicationCost();
                                updatedEventNum += t.getFilteredNum();
                                if(updatedMarkers == null){
                                    updatedMarkers = t.getUpdatedMarkers();
                                }else{
                                    updatedMarkers.merge(t.getUpdatedMarkers());
                                }
                            }
                        }else{
                            swf = null;
                            if(cacheFilterResult != null){
                                updatedEventNum += cacheFilterResult.filteredNum;
                                swf = SWF.deserialize(cacheFilterResult.updatedSWF.duplicate());
                            }
                            for (EqJoinFilterThread t : eqJoinFilterThreads) {
                                transmissionSize += t.getCommunicationCost();
                                updatedEventNum += t.getFilteredNum();
                                if(swf == null){
                                    swf = t.getUpdatedSWF();
                                }else{
                                    swf.or(t.getUpdatedSWF());
                                }
                            }
                        }
                        long endTime2 = System.currentTimeMillis();
                        System.out.println("current varName: " + varName + " build bf cost: " + (endTime2 - startTime2) + "ms");
                    }
                    else{
                        // only use window condition
                        long windowFilterStartTime = System.currentTimeMillis();
                        FilteredResult cacheFilterResult = null;
                        CacheWindowFilterThread cacheWindowFilterThread = null;
                        if(computeCache != null){
                            cacheWindowFilterThread = new CacheWindowFilterThread(
                                    computeCache, varName, window, query.headTailMarker(varName), swfBuffer);
                            cacheWindowFilterThread.start();
                        }

                        List<WindowFilterThread> windowFilterThreads = new ArrayList<>(nodeNum);

                        for (ExtendFilterBasedRPC.Client client : clients) {
                            WindowFilterThread thread = new WindowFilterThread(client, varName, window, query.headTailMarker(varName), swfBuffer);
                            thread.start();
                            windowFilterThreads.add(thread);
                        }
                        for (WindowFilterThread t : windowFilterThreads) {
                            try{
                                t.join();
                            }catch (Exception e){e.printStackTrace();}
                        }
                        if(cacheWindowFilterThread != null){
                            try{
                                cacheWindowFilterThread.join();
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            if(cacheWindowFilterThread.getError() != null){
                                throw cacheWindowFilterThread.getError();
                            }
                            cacheFilterResult = cacheWindowFilterThread.getFilteredResult();
                        }
                        logParallelPhase("window-filter-" + varName,
                                System.currentTimeMillis() - windowFilterStartTime,
                                cacheWindowFilterThread,
                                windowFilterThreads);
                        if(Args.isOptimizedSwf) {
                            updatedMarkers = null;
                            if(cacheFilterResult != null){
                                updatedEventNum += cacheFilterResult.filteredNum;
                                updatedMarkers = UpdatedMarkers.deserialize(cacheFilterResult.updatedSWF.duplicate());
                            }
                            for (WindowFilterThread t : windowFilterThreads) {
                                updatedEventNum += t.getFilteredNum();
                                transmissionSize += t.getCommunicationCost();
                                if(updatedMarkers == null){
                                    updatedMarkers = t.getUpdateMarkers();
                                }else{
                                    updatedMarkers.merge(t.getUpdateMarkers());
                                }
                            }
                        }
                        else{
                            swf = null;
                            if(cacheFilterResult != null){
                                updatedEventNum += cacheFilterResult.filteredNum;
                                swf = SWF.deserialize(cacheFilterResult.updatedSWF.duplicate());
                            }
                            for (WindowFilterThread t : windowFilterThreads) {
                                updatedEventNum += t.getFilteredNum();
                                if(swf == null){
                                    swf = t.getUpdatedSWF();
                                }else{
                                    swf.or(t.getUpdatedSWF());
                                }
                            }
                        }
                    }

                    if(Args.isOptimizedSwf){
                        optimizedSWF.rebuild(updatedMarkers);
                        slicedBits = optimizedSWF.getSliceNum();

                    }
                    else{
                        swf.rebuild();
                        slicedBits = swf.getSliceNum();
                    }
                }
                else{
                    updatedEventNum = varEventNumMap.get(varName);
                }
                filteredEventNum.put(varName, new Pair<>(updatedEventNum, slicedBits));
                hasProcessedVarName.add(varName);
            }
        }

        int recordLen = schema.getFixedRecordLen();
        ByteBuffer swfBuffer = Args.isOptimizedSwf ? optimizedSWF.serialize() : swf.serialize();
        long finalPullStartTime = System.currentTimeMillis();
        List<byte[]> cacheRecords = null;
        CachePullEventThread cachePullEventThread = null;
        if(computeCache != null){
            cachePullEventThread = new CachePullEventThread(computeCache, window, swfBuffer);
            cachePullEventThread.start();
        }

        List<PullEventThread> pullEventThreads = new ArrayList<>(nodeNum);
        for (ExtendFilterBasedRPC.Client client : clients) {
            PullEventThread thread = new PullEventThread(client, window, swfBuffer, recordLen);
            thread.start();
            pullEventThreads.add(thread);
        }
        for (PullEventThread thread : pullEventThreads){
            try{ thread.join(); } catch (Exception e){ e.printStackTrace(); }
        }
        if(cachePullEventThread != null){
            try{
                cachePullEventThread.join();
            }catch (Exception e){
                e.printStackTrace();
            }
            if(cachePullEventThread.getError() != null){
                throw cachePullEventThread.getError();
            }
            cacheRecords = cachePullEventThread.getAllEvents();
            // System.out.println("compute cache final event size: " + cacheRecords.size());
        }
        logParallelPhase("final-pull",
                System.currentTimeMillis() - finalPullStartTime,
                cachePullEventThread,
                pullEventThreads);

        List<byte[]> byteRecords = cacheRecords == null ? null : new ArrayList<>(cacheRecords);
        for (PullEventThread thread : pullEventThreads){
            transmissionSize += thread.getCommunicationCost();
            List<byte[]> storageEvents = thread.getAllEvents();
            if(storageEvents == null){
                continue;
            }
            if(byteRecords == null){
                byteRecords = storageEvents;
            }else{
                byteRecords.addAll(storageEvents);
            }
        }
        if(byteRecords == null){
            byteRecords = new ArrayList<>();
        }

        System.out.println("transmission cost: " + transmissionSize + " bytes");
        return byteRecords;
    }

    public static List<byte[]> communicate(String sql, List<ExtendFilterBasedRPC.Client> clients,
                                           ComputeNodeCache computeCacheStore){
        return communicate(sql, clients, computeCacheStore, FullScan.threadNum * Math.max(1, clients.size()));
    }

    public static void runQueries(List<ExtendFilterBasedRPC.Client> clients, String datasetName, boolean isEsper,
                                  ComputeNodeCache computeCacheStore, int queryLimit, int cacheThreadNum){
        EventSchema eventSchema = EventSchema.getEventSchema(datasetName);

        Schema schema = null;

        // esper: 1, flink: 2, sase: 3
        int ENGINE_ID;
        if(isEsper){
            ENGINE_ID = 1;
        }else if(Args.isSaseEngine){
            ENGINE_ID = 3;
        }else{
            ENGINE_ID = 2;
            schema = getSchemaByDatasetName(datasetName);
        }

        // strict continuous
        // List<String> sqlList = ReadQueries.getStrictConQueryList(datasetName);
        List<String> sqlList = ReadQueries.getQueryList(datasetName, false);
        List<String> esperSqlList = ReadQueries.getQueryList(datasetName, true);

        int queryCount = queryLimit > 0 ? Math.min(queryLimit, sqlList.size()) : sqlList.size();
        for(int i = 0; i < queryCount; i++) {
            System.out.println("query id: " + i);
            long queryStart = System.currentTimeMillis();
            String sql = sqlList.get(i);
            // System.out.println(sql);
            long startTime = System.currentTimeMillis();
            List<byte[]> byteRecords = communicate(sql, clients, computeCacheStore, cacheThreadNum);
            long endTime = System.currentTimeMillis();
            System.out.println("final event size: " + byteRecords.size());
            byteRecords = SortByTs.sort(byteRecords, eventSchema);

            System.out.println("pull event time: " + (endTime - startTime) + "ms");

            // EvaluationEngineSase.processQuery(byteRecords, sql, SelectionStrategy.STRICT_CONTIGUOUS);
            switch (ENGINE_ID){
                case 1:
                    String esperSql = esperSqlList.get(i);
                    switch (datasetName){
                        case "CRIMES":
                            List<EsperCrimes> esperCrimesEvents = byteRecords.stream().map(EsperCrimes::valueOf).collect(Collectors.toList());
                            EvaluationEngineEsper.processQuery(esperCrimesEvents, esperSql, datasetName);
                            break;
                        case "CITIBIKE":
                            List<EsperCitibike> esperCitibikeEvents = byteRecords.stream().map(EsperCitibike::valueOf).collect(Collectors.toList());
                            EvaluationEngineEsper.processQuery(esperCitibikeEvents, esperSql, datasetName);
                            break;
                        case "CLUSTER":
                            List<EsperCluster> esperClusterEvents = byteRecords.stream().map(EsperCluster::valueOf).collect(Collectors.toList());
                            EvaluationEngineEsper.processQuery(esperClusterEvents, esperSql, datasetName);
                            break;
                        default:
                            // ...
                    }
                    break;
                case 2:
                    switch (datasetName){
                        case "CRIMES":
                            List<CrimesEvent> crimesEvents = byteRecords.stream().map(CrimesEvent::valueOf).collect(Collectors.toList());
                            EvaluationEngineFlink.processQuery(crimesEvents, schema, sql, datasetName);
                            break;
                        case "CITIBIKE":
                            List<CitibikeEvent> citibikeEvents = byteRecords.stream().map(CitibikeEvent::valueOf).collect(Collectors.toList());
                            EvaluationEngineFlink.processQuery(citibikeEvents, schema, sql, datasetName);
                            break;
                        case "CLUSTER":
                            List<ClusterEvent> clusterEvents = byteRecords.stream().map(ClusterEvent::valueOf).collect(Collectors.toList());
                            // additional filtering operation
                            int idx1 = sql.indexOf("A.type = '");
                            String typeA = sql.substring(idx1 + 10, idx1 + 13);
                            int idx2 = sql.indexOf("B.type = '");
                            String typeB = sql.substring(idx2 + 10, idx2 + 13);
                            int idx3 = sql.indexOf("C.type = '");
                            String typeC = sql.substring(idx3 + 10, idx3 + 13);
                            long window = 3000000;
                            Map<Long, List<ReplayIntervals>> partitionIntervals =new HashMap<>(128);
                            int eventNum = clusterEvents.size();
                            for(int idx = 0; idx < eventNum; idx++){
                                ClusterEvent event = clusterEvents.get(idx);
                                String type = event.getType();
                                long jobId = event.getJOBID();
                                long ts = eventSchema.getTimestamp(byteRecords.get(idx));

                                if(!partitionIntervals.containsKey(jobId)){
                                    List<ReplayIntervals> list = new ArrayList<>(3);
                                    list.add(new ReplayIntervals(16));
                                    list.add(new ReplayIntervals(16));
                                    list.add(new ReplayIntervals(16));
                                    partitionIntervals.put(jobId, list);
                                }

                                if(type.equals(typeA)){
                                    partitionIntervals.get(jobId).get(0).insert(ts, ts + window);
                                }else if(type.equals(typeB)){
                                    partitionIntervals.get(jobId).get(1).insert(ts - window, ts + window);
                                }else if(type.equals(typeC)){
                                    partitionIntervals.get(jobId).get(2).insert(ts - window, ts);
                                }
                            }

                            ReplayIntervals finalRIS = new ReplayIntervals();
                            for(List<ReplayIntervals> lists : partitionIntervals.values()){
                                ReplayIntervals ris = lists.get(0);
                                ris.intersect(lists.get(1));
                                ris.intersect(lists.get(2));
                                finalRIS.union(ris);
                            }

                            List<ClusterEvent> filteredEvents = new ArrayList<>(eventNum >> 2);
                            for(int idx = 0; idx < eventNum; idx++){
                                if(finalRIS.contains(eventSchema.getTimestamp(byteRecords.get(idx)))){
                                    filteredEvents.add(clusterEvents.get(idx));
                                }
                            }
                            System.out.println("filteredEvents size: " + filteredEvents.size());
                            EvaluationEngineFlink.processQuery(filteredEvents, schema, sql, datasetName);
                            break;
                        default:
                            // ...
                    }
                    break;
                case 3:
                    EvaluationEngineSase.processQuery(byteRecords, sql);
            }
            long queryEnd = System.currentTimeMillis();
            System.out.println("this query cost: " + (queryEnd - queryStart) + "ms");
        }
    }

    public static void runQueries(List<ExtendFilterBasedRPC.Client> clients, String datasetName, boolean isEsper,
                                  ComputeNodeCache computeCacheStore){
        runQueries(clients, datasetName, isEsper, computeCacheStore, -1,
                FullScan.threadNum * Math.max(1, clients.size()));
    }

    private static Schema getSchemaByDatasetName(String datasetName){
        Schema schema;
        switch (datasetName){
            case "CRIMES":
                schema = Schema.newBuilder()
                        .column("type", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .column("caseNumber", DataTypes.STRING())
                        .column("IUCR", DataTypes.STRING())
                        .column("beat", DataTypes.INT())
                        .column("district", DataTypes.INT())
                        .column("latitude", DataTypes.DOUBLE())
                        .column("longitude", DataTypes.DOUBLE())
                        .column("FBICode", DataTypes.STRING())
                        .column("eventTime", DataTypes.TIMESTAMP(3))
                        .watermark("eventTime", "eventTime - INTERVAL '1' SECOND")
                        .build();
                break;
            case "CITIBIKE":
                schema = Schema.newBuilder()
                        .column("type", DataTypes.STRING())
                        .column("ride_id", DataTypes.BIGINT())
                        .column("start_station_id", DataTypes.INT())
                        .column("end_station_id", DataTypes.INT())
                        .column("start_lat", DataTypes.DOUBLE())
                        .column("start_lng", DataTypes.DOUBLE())
                        .column("end_lat", DataTypes.DOUBLE())
                        .column("end_lng", DataTypes.DOUBLE())
                        .column("eventTime", DataTypes.TIMESTAMP(3))
                        .watermark("eventTime", "eventTime - INTERVAL '1' SECOND")
                        .build();
                break;
            case "CLUSTER":
                schema = Schema.newBuilder()
                        .column("type", DataTypes.STRING())
                        .column("JOBID", DataTypes.BIGINT())
                        .column("index", DataTypes.INT())
                        .column("scheduling", DataTypes.STRING())
                        .column("priority", DataTypes.INT())
                        .column("CPU", DataTypes.FLOAT())
                        .column("RAM", DataTypes.FLOAT())
                        .column("DISK", DataTypes.FLOAT())
                        .column("eventTime", DataTypes.TIMESTAMP(3))
                        .watermark("eventTime", "eventTime - INTERVAL '1' SECOND")
                        .build();
                break;
            case "SYNTHETIC":
                schema = Schema.newBuilder()
                        .column("type", DataTypes.STRING())
                        .column("a1", DataTypes.INT())
                        .column("a2", DataTypes.STRING())
                        .column("a3", DataTypes.STRING())
                        .column("eventTime", DataTypes.TIMESTAMP(3))
                        .watermark("eventTime", "eventTime - INTERVAL '1' SECOND")
                        .build();
                break;
            default:
                throw new RuntimeException("without this schema");
        }
        return schema;
    }

    public static void main(String[] args) throws Exception {
        TConfiguration conf = new TConfiguration(Args.maxMassageLen, Args.maxMassageLen, Args.recursionLimit);
        predicateReplayIntervalIndex = new PredicateReplayIntervalIndex();

        String filename = stringArg(args, new String[]{"--filename", "filename", "--output", "output"}, "Compute.txt");
        redirectOutput(filename);

        String storageArg = stringArg(args, new String[]{"--storage", "storage"}, null);
        String[] storageNodeIps;
        int[] ports;
        if(storageArg == null || storageArg.trim().isEmpty()){
            storageNodeIps = stringListArg(args,
                    new String[]{"--storageNodeIps", "storageNodeIps", "--storage-node-ips", "storage-node-ips"},
                    new String[]{"44.211.118.126", "32.192.37.200", "44.204.96.38"});
            ports = intListArg(args, new String[]{"--ports", "ports"}, new int[]{9090, 9090, 9090});
        }else{
            StorageEndpoints endpoints = parseStorageEndpoints(storageArg);
            storageNodeIps = endpoints.hosts;
            ports = endpoints.ports;
        }
        if(storageNodeIps.length != ports.length){
            throw new IllegalArgumentException("storageNodeIps length must match ports length");
        }
        int nodeNum = storageNodeIps.length;
        int queryLimit = intArg(args, new String[]{"--query-limit", "queryLimit"}, -1);
        int cacheThreadNum = intArg(args,
                new String[]{"--cache-threads", "cacheThreads", "--cache-scan-threads", "cacheScanThreads",
                        "--compute-cache-scan-threads", "computeCacheScanThreads"},
                FullScan.threadNum * Math.max(1, nodeNum));
        int timeout = 0;
        List<TTransport> transports = new ArrayList<>(nodeNum);

        List<ExtendFilterBasedRPC.Client> clients = new ArrayList<>(nodeNum);
        for(int i = 0; i < nodeNum; ++i) {
            TSocket socket = new TSocket(conf, storageNodeIps[i], ports[i], timeout);
            TTransport transport = new TFramedTransport(socket, Args.maxMassageLen);
            TProtocol protocol = new TBinaryProtocol(transport);
            ExtendFilterBasedRPC.Client client = new ExtendFilterBasedRPC.Client(protocol);
            // when we open, we can call related interface
            transport.open();
            clients.add(client);
            transports.add(transport);
        }

        // "CRIMES", "CITIBIKE", "CLUSTER", "SYNTHETIC"
        String datasetName = stringArg(args, new String[]{"--dataset", "dataset", "--datasetName", "datasetName"}, "CITIBIKE");
        Args.computeCachePageNum = intArg(args,
                new String[]{"--cache-pages", "cachePages", "--compute-cache-pages", "computeCachePages"},
                8192);
        String engine = stringArg(args, new String[]{"--engine", "engine"}, Args.isSaseEngine ? "sase" : "flink");
        boolean isEsper = false;
        if("esper".equalsIgnoreCase(engine)){
            isEsper = true;
            Args.isSaseEngine = false;
        }else if("sase".equalsIgnoreCase(engine)){
            Args.isSaseEngine = true;
        }else if("flink".equalsIgnoreCase(engine)){
            Args.isSaseEngine = false;
        }else{
            throw new IllegalArgumentException("unsupported engine: " + engine);
        }
        System.out.println("@args #isEsper: " + isEsper + " #dataset: " + datasetName
                + " #storageNodeIps: " + String.join(",", storageNodeIps)
                + " #ports: " + portsToString(ports)
                + " #computeCachePageNum: " + Args.computeCachePageNum
                + " #cacheThreadNum: " + cacheThreadNum
                + " #queryLimit: " + queryLimit
                + " #filename: " + filename
                + " #isSaseEngine: " + Args.isSaseEngine);

        long cacheLoadStart = System.currentTimeMillis();
        ComputeNodeCache computeCacheStore = ComputeNodeCache.loadForExtend(datasetName, clients, Args.computeCachePageNum);
        long cacheLoadEnd = System.currentTimeMillis();
        if(computeCacheStore != null){
            System.out.println("compute cache load cost: " + (cacheLoadEnd - cacheLoadStart)
                    + "ms, bytes: " + computeCacheStore.getCachedBytes());
        }

        LocalDateTime now = LocalDateTime.now();
        System.out.println("Start time " + now);
        long start = System.currentTimeMillis();
        // please modify datasetName, isEsper to change running mode
        runQueries(clients, datasetName, isEsper, computeCacheStore, queryLimit, cacheThreadNum);
        long end = System.currentTimeMillis();
        now = LocalDateTime.now();
        System.out.println("Finish time " + now);
        System.out.println("Take " + (end - start) + "ms...");

        // we need to close transport
        for(TTransport transport : transports){
            transport.close();
        }
    }

    private static void redirectOutput(String filename) throws Exception {
        if(filename == null || filename.isEmpty()){
            return;
        }

        File outputFile = new File(filename);
        if(!outputFile.isAbsolute() && outputFile.getParentFile() == null){
            String sep = File.separator;
            outputFile = new File(System.getProperty("user.dir") + sep + "src" + sep + "main" + sep + "output", filename);
        }

        File parent = outputFile.getParentFile();
        if(parent != null && !parent.exists()){
            parent.mkdirs();
        }
        System.setOut(new PrintStream(outputFile));
    }

    private static String[] stringListArg(String[] args, String[] names, String[] defaultValue) {
        String value = stringArg(args, names, null);
        if(value == null || value.trim().isEmpty()){
            return defaultValue;
        }

        String[] parts = value.split(",");
        for(int i = 0; i < parts.length; i++){
            parts[i] = parts[i].trim();
            if(parts[i].isEmpty()){
                throw new IllegalArgumentException("empty item in argument: " + value);
            }
        }
        return parts;
    }

    private static int intArg(String[] args, String[] names, int defaultValue) {
        String value = stringArg(args, names, null);
        if(value == null || value.trim().isEmpty()){
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    private static int[] intListArg(String[] args, String[] names, int[] defaultValue) {
        String value = stringArg(args, names, null);
        if(value == null || value.trim().isEmpty()){
            return defaultValue;
        }

        String[] parts = value.split(",");
        int[] result = new int[parts.length];
        for(int i = 0; i < parts.length; i++){
            String item = parts[i].trim();
            if(item.isEmpty()){
                throw new IllegalArgumentException("empty item in argument: " + value);
            }
            result[i] = Integer.parseInt(item);
        }
        return result;
    }

    private static class StorageEndpoints {
        private final String[] hosts;
        private final int[] ports;

        private StorageEndpoints(String[] hosts, int[] ports) {
            this.hosts = hosts;
            this.ports = ports;
        }
    }

    private static StorageEndpoints parseStorageEndpoints(String value) {
        String[] parts = value.split(",");
        String[] hosts = new String[parts.length];
        int[] ports = new int[parts.length];
        for(int i = 0; i < parts.length; i++){
            String item = parts[i].trim();
            if(item.isEmpty()){
                throw new IllegalArgumentException("empty item in storage argument: " + value);
            }
            int colonIdx = item.lastIndexOf(':');
            if(colonIdx <= 0 || colonIdx == item.length() - 1){
                throw new IllegalArgumentException("storage item must be host:port, current item: " + item);
            }
            hosts[i] = item.substring(0, colonIdx);
            ports[i] = Integer.parseInt(item.substring(colonIdx + 1));
        }
        return new StorageEndpoints(hosts, ports);
    }

    private static String stringArg(String[] args, String[] names, String defaultValue) {
        for(String name : names){
            String value = findStringArg(args, name);
            if(value != null){
                return value;
            }
        }
        return defaultValue;
    }

    private static String findStringArg(String[] args, String name) {
        String prefix = name + "=";
        for(int i = 0; i < args.length; i++){
            if(args[i].startsWith(prefix)){
                return args[i].substring(prefix.length());
            }
            if(args[i].equals(name) && i + 1 < args.length){
                return args[i + 1];
            }
        }
        return null;
    }

    private static String portsToString(int[] ports) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < ports.length; i++){
            if(i > 0){
                sb.append(",");
            }
            sb.append(ports[i]);
        }
        return sb.toString();
    }
}
