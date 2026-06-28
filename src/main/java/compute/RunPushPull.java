package compute;

import event.*;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import parser.QueryParse;
import plan.GeneratedPlan;
import plan.Plan;
import request.ReadQueries;
import rpc.iface.PushPullDataChunk;
import rpc.iface.PushPullRPC;
import store.EventSchema;
import store.FullScan;
import utils.SortByTs;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class RunPushPull {
    private static final int DEFAULT_CACHE_PAGE_NUM = 8192;

    interface MeasuredTask {
        long getElapsedTime();
    }

    interface PullTask extends MeasuredTask {
        Map<String, List<byte[]>> getPulledEventsMap();
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
        List<MeasuredTask> cacheTasks = null;
        if(cacheTask != null){
            cacheTasks = new ArrayList<>(1);
            cacheTasks.add(cacheTask);
        }
        logParallelPhase(phaseName, wallTime, cacheTasks, storageTasks);
    }

    static void logParallelPhase(String phaseName, long wallTime, List<? extends MeasuredTask> cacheTasks,
                                 List<? extends MeasuredTask> storageTasks) {
        long cacheMaxTime = maxElapsedTime(cacheTasks);
        long cacheTotalTime = sumElapsedTime(cacheTasks);
        long storageMaxTime = maxElapsedTime(storageTasks);
        long storageTotalTime = sumElapsedTime(storageTasks);
        System.out.println("phase " + phaseName + " cost: wall=" + wallTime
                + "ms, cacheMax=" + cacheMaxTime
                + "ms, cacheTotal=" + cacheTotalTime
                + "ms, storageMax=" + storageMaxTime
                + "ms, storageTotal=" + storageTotalTime + "ms");
    }

    static class InitialThread extends Thread implements MeasuredTask {
        private final PushPullRPC.Client client;
        private final String tableName;
        private final String sql;
        private Map<String, Integer> varEventNumMap;
        private int communicatedByteSize;
        private long elapsedTime;

        public InitialThread(PushPullRPC.Client client, String tableName, String sql) {
            this.client = client;
            this.tableName = tableName;
            this.sql = sql;
            communicatedByteSize = tableName.length() + sql.length();
        }

        public int getCommunicatedByteSize(){
            return communicatedByteSize;
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
                varEventNumMap = client.initial(tableName, sql);
                communicatedByteSize += varEventNumMap.toString().length();
            }catch (Exception e) {
                e.printStackTrace();
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class ComputeCacheScanThread extends Thread implements MeasuredTask {
        private final ComputeNodeCache computeCacheStore;
        private final QueryParse query;
        private final int cacheThreadNum;
        private List<ComputeNodeCache> computeCaches;
        private RuntimeException error;
        private long elapsedTime;

        public ComputeCacheScanThread(ComputeNodeCache computeCacheStore, QueryParse query, int cacheThreadNum) {
            this.computeCacheStore = computeCacheStore;
            this.query = query;
            this.cacheThreadNum = cacheThreadNum;
        }

        public List<ComputeNodeCache> getComputeCaches() {
            return computeCaches;
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
            try{
                computeCaches = computeCacheStore.scanForPushPullCombined(query, cacheThreadNum);
            }catch (RuntimeException e){
                error = e;
            }finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    // directly pull events without matching
    static class SimplePullThread extends Thread implements PullTask {
        private final PushPullRPC.Client client;
        private final List<String> requestedVarNames;
        private final int recordSize;

        private Map<String, List<byte[]>> pulledEventsMap;
        private int communicationCost;
        private long elapsedTime;
        private RuntimeException error;

        public SimplePullThread(PushPullRPC.Client client, List<String> requestedVarNames, int recordSize) {
            this.client = client;
            this.requestedVarNames = requestedVarNames;
            this.recordSize = recordSize;
        }

        public int getCommunicatedByteSize(){
            return communicationCost;
        }

        public Map<String, List<byte[]>> getPulledEventsMap(){
            return pulledEventsMap;
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
            pulledEventsMap = new HashMap<>();  // reset map
            try{
                boolean isLastChunk;
                int offset = 0;
                do{
                    PushPullDataChunk chunk = client.getEventsByVarName(requestedVarNames, offset);
                    int maxLen = 0;
                    for(String varName : chunk.varEventMap.keySet()){
                        ByteBuffer buffer = chunk.varEventMap.get(varName);
                        communicationCost += buffer.capacity();

                        int recordNum = buffer.remaining() / recordSize;
                        List<byte[]> records = new ArrayList<>(recordNum);

                        for(int i = 0; i < recordNum; ++i){
                            byte[] record = new byte[recordSize];
                            buffer.get(record);
                            records.add(record);
                        }

                        if(pulledEventsMap.get(varName) == null){
                            pulledEventsMap.put(varName, records);
                        }else{
                            pulledEventsMap.get(varName).addAll(records);
                        }

                        maxLen = Math.max(maxLen, recordNum);

                    }

                    isLastChunk = chunk.isLastChunk;
                    offset += maxLen;
                }while(!isLastChunk);
            }catch (TException e){
                error = new RuntimeException("Failed to pull push-pull events from storage node", e);
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class CacheSimplePullThread extends Thread implements PullTask {
        private final ComputeNodeCache computeCache;
        private final List<String> requestedVarNames;
        private Map<String, List<byte[]>> pulledEventsMap;
        private RuntimeException error;
        private long elapsedTime;

        public CacheSimplePullThread(ComputeNodeCache computeCache, List<String> requestedVarNames) {
            this.computeCache = computeCache;
            this.requestedVarNames = requestedVarNames;
            this.pulledEventsMap = new HashMap<>();
        }

        public Map<String, List<byte[]>> getPulledEventsMap() {
            return pulledEventsMap;
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
            try{
                pulledEventsMap = computeCache.pullEventsForPushPull(requestedVarNames);
            }catch (RuntimeException e){
                error = e;
            }finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    // pull events with matching
    static class ComplexPullThread extends Thread implements PullTask {
        private final PushPullRPC.Client client;
        private final List<String> requestedVarNames;

        private final Map<String, List<byte[]>> eventListMap;
        private Map<String, List<byte[]>> pulledEvents;

        private int communicationCost;
        private final int recordSize;
        private long elapsedTime;
        private RuntimeException error;

        private int sendOffset = 0;

        public ComplexPullThread(PushPullRPC.Client client, List<String> requestedVarNames, Map<String, List<byte[]>> eventListMap, int recordSize) {
            this.client = client;
            this.requestedVarNames = requestedVarNames;
            this.eventListMap = eventListMap;
            this.recordSize = recordSize;
        }

        public int getCommunicatedByteSize(){
            return communicationCost;
        }

        public Map<String, List<byte[]>> getPulledEventsMap(){
            return pulledEvents;
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
            pulledEvents = new HashMap<>();     // reset
            sendOffset = 0;
            int chunkId = 0;
            try{
                boolean isLastChunk;
                int offset = 0;
                PushPullDataChunk chunk;

                // send data...
                do{
                    Map<String, Integer> recordNumMap = new HashMap<>();
                    for(Map.Entry<String, List<byte[]>> entry : eventListMap.entrySet()){
                        int curSize = entry.getValue() == null ? 0 : entry.getValue().size();
                        if(curSize > sendOffset){
                            recordNumMap.put(entry.getKey(), entry.getValue().size());
                        }
                    }
                    List<Map.Entry<String, Integer>> entryList = new ArrayList<>(recordNumMap.entrySet());
                    entryList.sort(Map.Entry.comparingByValue());

                    int hasUsedSize = 0;
                    int waitingProcessedVarNum = entryList.size();
                    Map<String, ByteBuffer> varEventMap = new HashMap<>(waitingProcessedVarNum << 1);

                    int maxReadOffset = -1;
                    for(Map.Entry<String, Integer> entry : entryList){
                        String varName = entry.getKey();
                        int remainingSize = (Args.MAX_CHUNK_SIZE - hasUsedSize) / waitingProcessedVarNum;
                        int consumeSize = (eventListMap.get(varName).size() - sendOffset) * recordSize;

                        if(consumeSize <= remainingSize){
                            ByteBuffer buffer = ByteBuffer.allocate(consumeSize);
                            List<byte[]> records = eventListMap.get(varName);
                            for(int i = sendOffset; i < records.size(); i++){
                                byte[] record = records.get(i);
                                buffer.put(record);
                            }
                            buffer.flip();
                            varEventMap.put(varName, buffer);
                            communicationCost += consumeSize;
                        }else{
                            if(maxReadOffset == -1){
                                maxReadOffset = remainingSize / recordSize;
                            }
                            ByteBuffer buffer = ByteBuffer.allocate(maxReadOffset * recordSize);
                            List<byte[]> records = eventListMap.get(varName);
                            for(int i = sendOffset; i < sendOffset + maxReadOffset; i++){
                                buffer.put(records.get(i));
                            }
                            buffer.flip();
                            varEventMap.put(varName, buffer);
                            hasUsedSize += maxReadOffset * recordSize;
                            communicationCost += maxReadOffset * recordSize;
                        }
                    }
                    if(maxReadOffset != -1){
                        sendOffset += maxReadOffset;
                    }

                    isLastChunk = maxReadOffset == -1;
                    if(varEventMap.isEmpty() && isLastChunk){
                        for(String varName : eventListMap.keySet()){
                            varEventMap.put(varName, ByteBuffer.allocate(0));
                        }
                    }
                    PushPullDataChunk sendChunk = new PushPullDataChunk(chunkId++, varEventMap, isLastChunk);
                    chunk = client.matchFilter(requestedVarNames, sendChunk, offset);
                    communicationCost += 4;
                }while (!isLastChunk);

                // receive data...
                boolean firstEntrance = true;
                do{
                    // special case processing
                    if(firstEntrance){
                        firstEntrance = false;
                    }else{
                        chunk = client.matchFilter(requestedVarNames, new PushPullDataChunk(-1, null, true), offset);
                    }

                    int maxOffset = 0;
                    Map<String, ByteBuffer> eventListMap = chunk.varEventMap;
                    if(eventListMap == null){
                        eventListMap = new HashMap<>(0);
                    }
                    for(String varName : eventListMap.keySet()){
                        ByteBuffer buffer = eventListMap.get(varName);
                        communicationCost += buffer.remaining();

                        int recordNum = buffer.remaining() / recordSize;
                        List<byte[]> records = new ArrayList<>(recordNum);
                        for(int i = 0; i < recordNum; ++i){
                            byte[] record = new byte[recordSize];
                            buffer.get(record);
                            records.add(record);
                        }


                        if(pulledEvents.containsKey(varName)){
                            pulledEvents.get(varName).addAll(records);
                        }else{
                            pulledEvents.put(varName, records);
                        }
                        maxOffset = Math.max(maxOffset, recordNum);
                    }
                    offset += maxOffset;
                }while(!chunk.isLastChunk);
            }catch (TException e){
                error = new RuntimeException("Failed to push-pull match/filter events from storage node", e);
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class CacheComplexPullThread extends Thread implements PullTask {
        private final ComputeNodeCache computeCache;
        private final List<String> requestedVarNames;
        private final Map<String, List<byte[]>> eventListMap;
        private Map<String, List<byte[]>> pulledEvents;
        private RuntimeException error;
        private long elapsedTime;

        public CacheComplexPullThread(ComputeNodeCache computeCache, List<String> requestedVarNames,
                                      Map<String, List<byte[]>> eventListMap) {
            this.computeCache = computeCache;
            this.requestedVarNames = requestedVarNames;
            this.eventListMap = eventListMap;
            this.pulledEvents = new HashMap<>();
        }

        public Map<String, List<byte[]>> getPulledEventsMap() {
            return pulledEvents;
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
            try{
                pulledEvents = computeCache.matchFilterForPushPull(requestedVarNames, eventListMap);
            }catch (RuntimeException e){
                error = e;
            }finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    public static List<byte[]> communicate(String sql, List<PushPullRPC.Client> clients,
                                           ComputeNodeCache computeCacheStore, int cacheThreadNum){
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        EventSchema schema = EventSchema.getEventSchema(tableName);
        int recordSize = schema.getFixedRecordLen();

        long transmissionSize = 0;

        int nodeNum = clients.size();
        // step 1: initial (process independent predicates and return cardinality)
        long initialStartTime = System.currentTimeMillis();
        ComputeCacheScanThread computeCacheScanThread = null;
        if(computeCacheStore != null){
            computeCacheScanThread = new ComputeCacheScanThread(computeCacheStore, query, cacheThreadNum);
            computeCacheScanThread.start();
        }

        List<InitialThread> initialThreads = new ArrayList<>(nodeNum);
        for(PushPullRPC.Client client : clients){
            InitialThread thread = new InitialThread(client, tableName, sql);
            thread.start();
            initialThreads.add(thread);
        }
        for(InitialThread thread : initialThreads){
            try { thread.join(); }catch (Exception e) { e.printStackTrace(); }
        }
        List<ComputeNodeCache> computeCaches = null;
        if(computeCacheScanThread != null){
            try{
                computeCacheScanThread.join();
            }catch (Exception e){
                e.printStackTrace();
            }
            if(computeCacheScanThread.getError() != null){
                throw computeCacheScanThread.getError();
            }
            computeCaches = computeCacheScanThread.getComputeCaches();
            System.out.println("compute cache push-pull shards: " + computeCaches.size()
                    + ", cardinality: " + aggregateCardinality(computeCaches));
            System.out.println("compute cache push-pull scan cost: " + computeCacheScanThread.getElapsedTime()
                    + "ms, threadsPerNode=" + cacheThreadNum);
        }
        logParallelPhase("initial", System.currentTimeMillis() - initialStartTime, computeCacheScanThread, initialThreads);

        // aggregate number of events
        Map<String, Integer> varEventNumMap = new HashMap<>();
        if(computeCaches != null){
            for(ComputeNodeCache computeCache : computeCaches){
                mergeCardinality(varEventNumMap, computeCache.getCardinality());
            }
        }
        for(InitialThread thread : initialThreads){
            transmissionSize += thread.getCommunicatedByteSize();
            Map<String, Integer> map = thread.getVarEventNumMap();
            mergeCardinality(varEventNumMap, map);
        }
        System.out.println("varEventNumMap: " + varEventNumMap);
        Map<String, List<byte[]>> eventMap = new HashMap<>();

        // here we should choose
        query.getDpMap();
        Plan plan  = GeneratedPlan.pullPushPlan(varEventNumMap, query.getDpMap());
        List<String> steps = plan.getSteps();
        System.out.println("steps: " + steps);
        Set<String> hasProcessedVarName = new HashSet<>();
        String lastVarName = "";
        for(String varName : steps){
            List<String> requestedVarNames = new ArrayList<>(4);
            requestedVarNames.add(varName);
            if(hasProcessedVarName.isEmpty()) {
                long simplePullStartTime = System.currentTimeMillis();
                List<CacheSimplePullThread> cacheSimplePullThreads = new ArrayList<>();
                if(computeCaches != null){
                    for(ComputeNodeCache computeCache : computeCaches){
                        CacheSimplePullThread thread = new CacheSimplePullThread(computeCache, requestedVarNames);
                        thread.start();
                        cacheSimplePullThreads.add(thread);
                    }
                }

                List<SimplePullThread> threads = new ArrayList<>(nodeNum);
                for (PushPullRPC.Client client : clients) {
                    SimplePullThread thread = new SimplePullThread(client, requestedVarNames, recordSize);
                    thread.start();
                    threads.add(thread);
                }
                for (SimplePullThread thread : threads) {
                    try {
                        thread.join();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                for(CacheSimplePullThread thread : cacheSimplePullThreads){
                    try {
                        thread.join();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if(thread.getError() != null){
                        throw thread.getError();
                    }
                    mergePulledEvents(eventMap, thread.getPulledEventsMap(), schema);
                }
                if(!cacheSimplePullThreads.isEmpty()){
                    System.out.println("compute cache simple pull event size: " + countEvents(cacheSimplePullThreads));
                }
                logParallelPhase("simple-pull-" + varName,
                        System.currentTimeMillis() - simplePullStartTime,
                        cacheSimplePullThreads,
                        threads);

                // union all events
                for (SimplePullThread thread : threads) {
                    if(thread.getError() != null){
                        throw thread.getError();
                    }
                    transmissionSize += thread.getCommunicatedByteSize();
                    mergePulledEvents(eventMap, thread.getPulledEventsMap(), schema);
                }
            }
            else{
                List<byte[]> varEventList = eventMap.get(lastVarName);
                Map<String, List<byte[]>> sendMap = new HashMap<>();
                sendMap.put(lastVarName, varEventList == null ? new ArrayList<byte[]>(0) : varEventList);

                long complexPullStartTime = System.currentTimeMillis();
                List<CacheComplexPullThread> cacheComplexPullThreads = new ArrayList<>();
                if(computeCaches != null){
                    for(ComputeNodeCache computeCache : computeCaches){
                        CacheComplexPullThread thread = new CacheComplexPullThread(computeCache, requestedVarNames, sendMap);
                        thread.start();
                        cacheComplexPullThreads.add(thread);
                    }
                }

                List<ComplexPullThread> threads = new ArrayList<>(nodeNum);
                for(PushPullRPC.Client client : clients){
                    ComplexPullThread thread = new ComplexPullThread(client, requestedVarNames, sendMap, recordSize);
                    thread.start();
                    threads.add(thread);
                }
                for(ComplexPullThread thread : threads){
                    try { thread.join(); }catch (Exception e) { e.printStackTrace(); }
                }
                for(CacheComplexPullThread thread : cacheComplexPullThreads){
                    try { thread.join(); }catch (Exception e) { e.printStackTrace(); }
                    if(thread.getError() != null){
                        throw thread.getError();
                    }
                    mergePulledEvents(eventMap, thread.getPulledEventsMap(), schema);
                }
                if(!cacheComplexPullThreads.isEmpty()){
                    System.out.println("compute cache complex pull event size: " + countEvents(cacheComplexPullThreads));
                }
                logParallelPhase("complex-pull-" + varName,
                        System.currentTimeMillis() - complexPullStartTime,
                        cacheComplexPullThreads,
                        threads);

                for(ComplexPullThread thread : threads) {
                    if(thread.getError() != null){
                        throw thread.getError();
                    }
                    transmissionSize += thread.getCommunicatedByteSize();
                    mergePulledEvents(eventMap, thread.getPulledEventsMap(), schema);
                }
            }
            hasProcessedVarName.add(varName);
            lastVarName = varName;
        }

        List<byte[]> filteredEvents = null;
        for(List<byte[]> entry : eventMap.values()){
            filteredEvents = merge(filteredEvents, entry, schema);
        }
        if(filteredEvents == null){
            filteredEvents = new ArrayList<>();
        }
        System.out.println("transmission cost: " + transmissionSize + " bytes");
        return filteredEvents;
    }

    public static List<byte[]> communicate(String sql, List<PushPullRPC.Client> clients){
        return communicate(sql, clients, null);
    }

    public static List<byte[]> communicate(String sql, List<PushPullRPC.Client> clients,
                                           ComputeNodeCache computeCacheStore){
        return communicate(sql, clients, computeCacheStore, FullScan.threadNum);
    }

    private static void mergeCardinality(Map<String, Integer> totalMap, Map<String, Integer> addMap) {
        if(addMap == null){
            throw new IllegalStateException("push-pull initial phase returned null cardinality map");
        }
        for(Map.Entry<String, Integer> entry : addMap.entrySet()){
            String key = entry.getKey();
            int value = entry.getValue();
            totalMap.put(key, totalMap.getOrDefault(key, 0) + value);
        }
    }

    private static Map<String, Integer> aggregateCardinality(List<ComputeNodeCache> computeCaches) {
        Map<String, Integer> cardinality = new HashMap<>();
        if(computeCaches == null){
            return cardinality;
        }
        for(ComputeNodeCache computeCache : computeCaches){
            mergeCardinality(cardinality, computeCache.getCardinality());
        }
        return cardinality;
    }

    private static void mergePulledEvents(Map<String, List<byte[]>> eventMap,
                                          Map<String, List<byte[]>> pulledEvents,
                                          EventSchema schema) {
        if(pulledEvents == null){
            return;
        }
        for(Map.Entry<String, List<byte[]>> entry : pulledEvents.entrySet()){
            String key = entry.getKey();
            List<byte[]> list = entry.getValue();
            if(list == null){
                list = new ArrayList<>(0);
            }
            eventMap.put(key, merge(eventMap.get(key), list, schema));
        }
    }

    private static int countEvents(Map<String, List<byte[]>> eventsMap) {
        int count = 0;
        if(eventsMap == null){
            return count;
        }
        for(List<byte[]> events : eventsMap.values()){
            if(events != null){
                count += events.size();
            }
        }
        return count;
    }

    private static int countEvents(List<? extends PullTask> tasks) {
        int count = 0;
        if(tasks == null){
            return count;
        }
        for(PullTask task : tasks){
            count += countEvents(task.getPulledEventsMap());
        }
        return count;
    }

    public static void runQueries(List<PushPullRPC.Client> clients, String datasetName, boolean isEsper,
                                  ComputeNodeCache computeCacheStore, int queryLimit, int cacheThreadNum){
        runQueries(clients, datasetName, isEsper, computeCacheStore, queryLimit, cacheThreadNum, 0);
    }

    public static void runQueries(List<PushPullRPC.Client> clients, String datasetName, boolean isEsper,
                                  ComputeNodeCache computeCacheStore, int queryLimit, int cacheThreadNum,
                                  int queryStartIndex){
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

        List<String> sqlList = ReadQueries.getQueryList(datasetName, false);
        List<String> esperSqlList = ReadQueries.getQueryList(datasetName, true);

        // sqlList.size()
        int startIndex = Math.max(0, queryStartIndex);
        if(startIndex > sqlList.size()){
            throw new IllegalArgumentException("queryStart exceeds query count: " + startIndex);
        }
        int endIndex = queryLimit > 0 ? Math.min(startIndex + queryLimit, sqlList.size()) : sqlList.size();
        for(int i = startIndex; i < endIndex; i++){
            System.out.println("query id: " + i);
            long queryStart = System.currentTimeMillis();
            String sql = sqlList.get(i);
            // System.out.println(sql);
            long startTime = System.currentTimeMillis();
            List<byte[]> byteRecords = communicate(sql, clients, computeCacheStore, cacheThreadNum);
            long endTime = System.currentTimeMillis();
            System.out.println("final event size: " + byteRecords.size());

            // note that we do not sort operation since each list are ordered
            // byteRecords = SortByTs.sort(byteRecords, eventSchema);
            System.out.println("pull event time: " + (endTime - startTime) + "ms");

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
                            EvaluationEngineFlink.processQuery(clusterEvents, schema, sql, datasetName);
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

    public static void runQueries(List<PushPullRPC.Client> clients, String datasetName, boolean isEsper,
                                  ComputeNodeCache computeCacheStore, int queryLimit){
        runQueries(clients, datasetName, isEsper, computeCacheStore, queryLimit, FullScan.threadNum);
    }

    public static void runQueries(List<PushPullRPC.Client> clients, String datasetName, boolean isEsper){
        runQueries(clients, datasetName, isEsper, null, -1);
    }

    // two sorted list
    public static List<byte[]> merge(List<byte[]> list1, List<byte[]> list2, EventSchema schema){
        if(list1 == null || list1.isEmpty()){
            return list2;
        }
        int size1 = list1.size();
        int size2 = list2.size();

        if(size2 > 0 && schema.getTimestamp(list1.get(size1 - 1)) < schema.getTimestamp(list2.get(0))){
            list1.addAll(list2);
            return list1;
        }

        List<byte[]> mergedList = new ArrayList<>(size1 + size2);

        int i = 0, j = 0;
        while(i < size1 && j < size2){
            long ts1 = schema.getTimestamp(list1.get(i));
            long ts2 = schema.getTimestamp(list2.get(j));
            if(ts1 < ts2){
                mergedList.add(list1.get(i));
                i++;
            }else if(ts2 < ts1){
                mergedList.add(list2.get(j));
                j++;
            }else{
                // same timestamp
                if(!Arrays.equals(list1.get(i), list2.get(j))){
                    mergedList.add(list2.get(j));
                }
                j++;
            }
        }

        while(i < size1){
            mergedList.add(list1.get(i));
            i++;
        }

        while(j < size2){
            mergedList.add(list2.get(j));
            j++;
        }
        return mergedList;
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
        int queryStart = intArg(args, new String[]{"--query-start", "queryStart"}, 0);
        int cacheThreadNum = intArg(args,
                new String[]{"--cache-threads", "cacheThreads", "--cache-scan-threads", "cacheScanThreads",
                        "--compute-cache-scan-threads", "computeCacheScanThreads"},
                FullScan.threadNum);
        int timeout = 0;
        List<TTransport> transports = new ArrayList<>(nodeNum);
        List<PushPullRPC.Client> clients = new ArrayList<>(nodeNum);

        for(int i = 0; i < nodeNum; ++i) {
            TSocket socket = new TSocket(conf, storageNodeIps[i], ports[i], timeout);
            TTransport transport = new TFramedTransport(socket, Args.maxMassageLen);
            TProtocol protocol = new TBinaryProtocol(transport);
            PushPullRPC.Client client = new PushPullRPC.Client(protocol);
            // when we open, we can call related interface
            transport.open();
            clients.add(client);
            transports.add(transport);
        }

        // "CRIMES", "CITIBIKE", "CLUSTER", "SYNTHETIC"
        String datasetName = stringArg(args, new String[]{"--dataset", "dataset", "--datasetName", "datasetName"}, "CITIBIKE");
        Args.computeCachePageNum = intArg(args,
                new String[]{"--cache-pages", "cachePages", "--compute-cache-pages", "computeCachePages"},
                DEFAULT_CACHE_PAGE_NUM);
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
                + " #queryStart: " + queryStart
                + " #filename: " + filename
                + " #isSaseEngine: " + Args.isSaseEngine);

        long cacheLoadStart = System.currentTimeMillis();
        ComputeNodeCache computeCacheStore = ComputeNodeCache.loadForPushPull(datasetName, clients, Args.computeCachePageNum);
        long cacheLoadEnd = System.currentTimeMillis();
        if(computeCacheStore != null){
            System.out.println("push-pull compute cache load cost: " + (cacheLoadEnd - cacheLoadStart)
                    + "ms, bytes: " + computeCacheStore.getCachedBytes());
        }

        LocalDateTime now = LocalDateTime.now();
        System.out.println("Start time " + now);
        long start = System.currentTimeMillis();
        // please modify datasetName, isEsper to change running mode
        runQueries(clients, datasetName, isEsper, computeCacheStore, queryLimit, cacheThreadNum, queryStart);
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
