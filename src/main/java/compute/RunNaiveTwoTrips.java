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
import parser.EqualDependentPredicate;
import parser.QueryParse;
import request.ReadQueries;
import rpc.iface.DataChunk;
import rpc.iface.TsAttrPair;
import rpc.iface.TwoTripsDataChunk;
import rpc.iface.TwoTripsRPC;
import store.EventSchema;
import store.FullScan;
import utils.ReplayIntervals;
import utils.SortByTs;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class RunNaiveTwoTrips {
    private static final int DEFAULT_CACHE_PAGE_NUM = 8192;
    private static int computeCacheScanThreadNum = FullScan.threadNum;

    interface MeasuredTask {
        long getElapsedTime();
    }

    static long maxElapsedTime(List<? extends MeasuredTask> tasks) {
        long max = 0;
        for(MeasuredTask task : tasks){
            max = Math.max(max, task.getElapsedTime());
        }
        return max;
    }

    static void logParallelPhase(String phaseName, long wallTime, MeasuredTask cacheTask,
                                 List<? extends MeasuredTask> storageTasks) {
        long cacheTime = cacheTask == null ? 0 : cacheTask.getElapsedTime();
        long storageMaxTime = maxElapsedTime(storageTasks);
        System.out.println("phase " + phaseName + " cost: wall=" + wallTime
                + "ms, cache=" + cacheTime + "ms, storageMax=" + storageMaxTime + "ms");
    }

    static class FirstTripThread extends Thread implements MeasuredTask {
        private final TwoTripsRPC.Client client;
        private final String tableName;
        private final String sql;

        private Map<String, ReplayIntervals> replayIntervalMap;
        private Map<String, Set<TsAttrPair>> tsAttrPairMap;
        private long communicationCost;
        private long elapsedTime;
        private RuntimeException error;

        public FirstTripThread(TwoTripsRPC.Client client, String tableName, String sql){
            this.client = client;
            this.tableName = tableName;
            this.sql = sql;

            replayIntervalMap = new HashMap<>(8);
            tsAttrPairMap = new HashMap<>(8);
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public Map<String, ReplayIntervals> getReplayIntervalMap(){
            return replayIntervalMap;
        }

        public Map<String, Set<TsAttrPair>> getTsAttrPairMap(){
            return tsAttrPairMap;
        }

        public RuntimeException getError() {
            return error;
        }

        @Override
        public long getElapsedTime() {
            return elapsedTime;
        }

        @Override
        public void run(){
            long startTime = System.currentTimeMillis();
            try{
                boolean isLastChunk;
                int offset = 0;
                do{
                    TwoTripsDataChunk chunk = client.pullBasicInfo(tableName, sql, offset);
                    if(chunk.intervalMap != null){
                        for(String varName : chunk.intervalMap.keySet()){
                            ByteBuffer buffer = chunk.intervalMap.get(varName).duplicate();
                            ReplayIntervals replayIntervals = ReplayIntervals.deserialize(buffer);
                            replayIntervalMap.put(varName, replayIntervals);
                            communicationCost += buffer.capacity() + varName.length();
                        }
                    }

                    int maxLen = 0;
                    if(chunk.pairMap != null && !chunk.pairMap.isEmpty()){
                        for(String varName : chunk.pairMap.keySet()){
                            Set<TsAttrPair> pairs = chunk.pairMap.get(varName);
                            if(tsAttrPairMap.get(varName) == null){
                                tsAttrPairMap.put(varName, pairs);
                            }else{
                                tsAttrPairMap.get(varName).addAll(pairs);
                            }
                            maxLen = Math.max(maxLen, pairs.size());

                            for(TsAttrPair item : pairs){
                                communicationCost += (8 + item.attrValue.length());
                            }
                        }
                    }
                    offset += maxLen;
                    isLastChunk = chunk.isLastChunk;
                    communicationCost += tableName.length() + sql.length() + 4;
                }while(!isLastChunk);

            }catch (TException e){
                error = new RuntimeException("Failed during two-trips first trip", e);
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class SecondTripThread extends Thread implements MeasuredTask {
        private final TwoTripsRPC.Client client;
        private final ByteBuffer replayIntervalsBuffer;

        private final int recordSize;
        private List<byte[]> events;
        private long communicationCost;
        private long elapsedTime;
        private RuntimeException error;

        public SecondTripThread(TwoTripsRPC.Client client, ByteBuffer replayIntervalsBuffer, int recordSize){
            this.client = client;
            this.replayIntervalsBuffer = replayIntervalsBuffer.asReadOnlyBuffer();
            this.recordSize = recordSize;
            events = new ArrayList<>(4096);
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public List<byte[]> getEvents(){
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
        public void run(){
            long startTime = System.currentTimeMillis();
            try {
                // we assume that without package loss, omit chunk id
                boolean isLastChunk;
                int offset = 0;
                do{
                    DataChunk chunk = client.pullEvents(offset, replayIntervalsBuffer.duplicate());
                    communicationCost += 4 + replayIntervalsBuffer.capacity();
                    communicationCost += chunk.data.capacity();

                    int recordNum = chunk.data.remaining() / recordSize;
                    for(int i = 0; i < recordNum; ++i){
                        byte[] record = new byte[recordSize];
                        chunk.data.get(record);
                        events.add(record);
                    }
                    isLastChunk = chunk.isLastChunk;

                    offset += recordNum;
                }while(!isLastChunk);
            } catch (TException e) {
                error = new RuntimeException("Failed during two-trips second trip", e);
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class ComputeCacheFirstTripThread extends Thread implements MeasuredTask {
        private final ComputeNodeCache computeCacheStore;
        private final String sql;
        private ComputeNodeCache computeCache;
        private Map<String, ReplayIntervals> replayIntervalMap;
        private Map<String, Set<TsAttrPair>> tsAttrPairMap;
        private Map<String, Integer> cardinalityMap;
        private long scanCost;
        private long intervalCost;
        private long pairCost;
        private long elapsedTime;
        private RuntimeException error;

        ComputeCacheFirstTripThread(ComputeNodeCache computeCacheStore, String sql) {
            this.computeCacheStore = computeCacheStore;
            this.sql = sql;
            this.replayIntervalMap = new HashMap<>(8);
            this.tsAttrPairMap = new HashMap<>(8);
            this.cardinalityMap = new HashMap<>(8);
        }

        public ComputeNodeCache getComputeCache() {
            return computeCache;
        }

        public Map<String, ReplayIntervals> getReplayIntervalMap() {
            return replayIntervalMap;
        }

        public Map<String, Set<TsAttrPair>> getTsAttrPairMap() {
            return tsAttrPairMap;
        }

        public Map<String, Integer> getCardinalityMap() {
            return cardinalityMap;
        }

        public RuntimeException getError() {
            return error;
        }

        public long getScanCost() {
            return scanCost;
        }

        public long getIntervalCost() {
            return intervalCost;
        }

        public long getPairCost() {
            return pairCost;
        }

        @Override
        public long getElapsedTime() {
            return elapsedTime;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try{
                QueryParse query = new QueryParse(sql);
                long scanStart = System.currentTimeMillis();
                computeCache = computeCacheStore.scanInParallel(query.getIpStringMap(), computeCacheScanThreadNum);
                scanCost = System.currentTimeMillis() - scanStart;
                cardinalityMap = computeCache.getCardinality();
                long intervalStart = System.currentTimeMillis();
                replayIntervalMap = computeCache.getTwoTripsReplayIntervals(query);
                intervalCost = System.currentTimeMillis() - intervalStart;
                long pairStart = System.currentTimeMillis();
                tsAttrPairMap = computeCache.getTwoTripsTsAttrPairs(query);
                pairCost = System.currentTimeMillis() - pairStart;
            }catch (RuntimeException e){
                error = e;
            }finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class ComputeCacheSecondTripThread extends Thread implements MeasuredTask {
        private final ComputeNodeCache computeCache;
        private final ReplayIntervals replayIntervals;
        private List<byte[]> events;
        private long elapsedTime;
        private RuntimeException error;

        ComputeCacheSecondTripThread(ComputeNodeCache computeCache, ReplayIntervals replayIntervals) {
            this.computeCache = computeCache;
            this.replayIntervals = replayIntervals;
            this.events = new ArrayList<>(4096);
        }

        public List<byte[]> getEvents() {
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
            try{
                events = computeCache.getRecords(replayIntervals);
            }catch (RuntimeException e){
                error = e;
            }finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static void mergeReplayIntervalMap(Map<String, ReplayIntervals> totalMap,
                                       Map<String, ReplayIntervals> addMap) {
        for(Map.Entry<String, ReplayIntervals> entry : addMap.entrySet()){
            String varName = entry.getKey();
            ReplayIntervals replayIntervals = entry.getValue();
            if(totalMap.get(varName) == null){
                totalMap.put(varName, replayIntervals);
            }else{
                totalMap.get(varName).union(replayIntervals);
            }
        }
    }

    static void mergeTsAttrPairMap(Map<String, Set<TsAttrPair>> totalMap,
                                   Map<String, Set<TsAttrPair>> addMap) {
        for(Map.Entry<String, Set<TsAttrPair>> entry : addMap.entrySet()){
            String varName = entry.getKey();
            Set<TsAttrPair> pairs = entry.getValue();
            if(totalMap.get(varName) == null){
                totalMap.put(varName, new HashSet<>(pairs));
            }else{
                totalMap.get(varName).addAll(pairs);
            }
        }
    }

    public static List<byte[]> communicate(String sql, List<TwoTripsRPC.Client> clients,
                                           ComputeNodeCache computeCacheStore){
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        long transmissionSize = 0;

        int nodeNum = clients.size();
        EventSchema schema = EventSchema.getEventSchema(tableName);
        int recordSize = schema.getFixedRecordLen();

        long firstTripStartTime = System.currentTimeMillis();
        ComputeCacheFirstTripThread computeCacheFirstTripThread = null;
        if(computeCacheStore != null){
            computeCacheFirstTripThread = new ComputeCacheFirstTripThread(computeCacheStore, sql);
            computeCacheFirstTripThread.start();
        }

        List<FirstTripThread> firstTripThreads = new ArrayList<>(nodeNum);
        for(TwoTripsRPC.Client client : clients){
            FirstTripThread thread = new FirstTripThread(client, tableName, sql);
            thread.start();
            firstTripThreads.add(thread);
        }
        for(FirstTripThread t : firstTripThreads){
            try{
                t.join();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        if(computeCacheFirstTripThread != null){
            try{
                computeCacheFirstTripThread.join();
            }catch (Exception e){
                e.printStackTrace();
            }
            if(computeCacheFirstTripThread.getError() != null){
                throw computeCacheFirstTripThread.getError();
            }
            System.out.println("compute cache two-trips cardinality: "
                    + computeCacheFirstTripThread.getCardinalityMap());
            System.out.println("compute cache two-trips profile: scan="
                    + computeCacheFirstTripThread.getScanCost() + "ms, intervals="
                    + computeCacheFirstTripThread.getIntervalCost() + "ms, pairs="
                    + computeCacheFirstTripThread.getPairCost() + "ms, threads="
                    + computeCacheScanThreadNum);
        }
        for(FirstTripThread t : firstTripThreads){
            if(t.getError() != null){
                throw t.getError();
            }
        }
        logParallelPhase("two-trips-first",
                System.currentTimeMillis() - firstTripStartTime,
                computeCacheFirstTripThread,
                firstTripThreads);

        //long time1Start = System.currentTimeMillis();
        // union replay intervals
        Map<String, ReplayIntervals> replayIntervalsMap = new HashMap<>(8);
        if(computeCacheFirstTripThread != null){
            mergeReplayIntervalMap(replayIntervalsMap, computeCacheFirstTripThread.getReplayIntervalMap());
        }
        for(FirstTripThread t : firstTripThreads){
            transmissionSize += t.getCommunicationCost();
            mergeReplayIntervalMap(replayIntervalsMap, t.getReplayIntervalMap());
        }
        if(replayIntervalsMap.isEmpty()){
            System.out.println("transmission cost: " + transmissionSize + " bytes");
            return new ArrayList<>();
        }

        Map<String, Long> varIntervalLenMap = new HashMap<>(replayIntervalsMap.size() << 1);

        ReplayIntervals finalIntervals = null;
        for(Map.Entry<String, ReplayIntervals> entry : replayIntervalsMap.entrySet()){
            String varName = entry.getKey();
            ReplayIntervals curIntervals = entry.getValue();
            varIntervalLenMap.put(varName, curIntervals.getTimeLength());
            // debug
            // System.out.println("varName: " + varName + " length: " + curIntervals.getTimeLength());

            if(finalIntervals == null){
                finalIntervals = curIntervals;
            }else{
                finalIntervals.intersect(curIntervals);
                // debug
                // System.out.println("updated length: " + finalIntervals.getTimeLength());
            }
        }

        // union <timestamp, attribute value> pairs
        Map<String, Set<TsAttrPair>> tsAttrPairMap = new HashMap<>(8);
        if(computeCacheFirstTripThread != null){
            mergeTsAttrPairMap(tsAttrPairMap, computeCacheFirstTripThread.getTsAttrPairMap());
        }
        for(FirstTripThread t : firstTripThreads){
            mergeTsAttrPairMap(tsAttrPairMap, t.getTsAttrPairMap());
        }

        long window = query.getWindow();
        Map<String, Map<Long, Set<String>>> windowAttrMap = new HashMap<>(tsAttrPairMap.size() << 1);
        for(Map.Entry<String, Set<TsAttrPair>> entry : tsAttrPairMap.entrySet()){
            String varAttrName = entry.getKey();
            Set<TsAttrPair> pairs = entry.getValue();
            // check timestamp...
            for(TsAttrPair pair : pairs){
                long ts = pair.timestamp;
                if(finalIntervals.contains(ts)) {
                    windowAttrMap.computeIfAbsent(varAttrName, k -> new HashMap<>()).computeIfAbsent(ts / window, k -> new HashSet<>()).add(pair.attrValue);
                }
            }
        }

        // we only consider A.x = B.y, do not consider A.x = B.y + 1
        Map<String, List<String>> equalDpStrMap = query.getEqualDpMap();

        // define process order
        List<Map.Entry<String, Long>> entryList = new ArrayList<>(varIntervalLenMap.entrySet());
        entryList.sort(Map.Entry.comparingByValue());
        Set<String> hasProcessedVarNames = new HashSet<>(8);
        hasProcessedVarNames.add(entryList.get(0).getKey());

        for(int i = 1; i < entryList.size(); i++){
            String varName1 = entryList.get(i).getKey();

            long leftOffset, rightOffset;
            if(query.headTailMarker(varName1) == 0){
                // 0: head variable, 1: tail variable, 2: middle variable
                leftOffset = 0;
                rightOffset = window;
            }else if(query.headTailMarker(varName1) == 1){
                leftOffset = -window;
                rightOffset = 0;
            }else{
                leftOffset = -window;
                rightOffset = window;
            }

            for(String varName2 : hasProcessedVarNames){
                String key = varName1.compareTo(varName2) < 0 ? (varName1 + "-" + varName2) : (varName2 + "-" + varName1);
                if(equalDpStrMap.containsKey(key)){
                    // false: previous, true: next
                    boolean isNext = query.compareSequence(varName2, varName1);
                    List<String> equalDpList = equalDpStrMap.get(key);

                    for(String equalDp : equalDpList){
                        ReplayIntervals newIntervals = new ReplayIntervals();
                        EqualDependentPredicate edp = new EqualDependentPredicate(equalDp);
                        String key1 = varName1 + "-" + edp.getAttrName(varName1);
                        String key2 = varName2 + "-" + edp.getAttrName(varName2);

                        int fixOffset = isNext ? 1 : -1;
                        Set<TsAttrPair> pairs = tsAttrPairMap.get(key1);
                        if(pairs == null || pairs.isEmpty()){
                            finalIntervals.intersect(new ReplayIntervals());
                            continue;
                        }
                        for(TsAttrPair pair : pairs){
                            long ts = pair.timestamp;
                            Map<Long, Set<String>> joinMap = windowAttrMap.get(key2);
                            if(joinMap != null && finalIntervals.contains(ts)){
                                long wid = ts/window;
                                if((joinMap.containsKey(wid) && joinMap.get(wid).contains(pair.attrValue)) ||
                                        (joinMap.containsKey(wid + fixOffset) && joinMap.get(wid + fixOffset).contains(pair.attrValue))){
                                    newIntervals.insert(ts + leftOffset, ts + rightOffset);
                                }
                            }
                        }

                        finalIntervals.intersect(newIntervals);

                        // debug
                        // System.out.println("equal predicate -> interval len: " + finalIntervals.getTimeLength());
                    }
                }
            }
            hasProcessedVarNames.add(varName1);
        }

        ByteBuffer buffer = finalIntervals.serialize();
        //long time1End = System.currentTimeMillis();
        //System.out.println("determine replay intervals cost: " + (time1End - time1Start) + "ms");

        long secondTripStartTime = System.currentTimeMillis();
        ComputeCacheSecondTripThread computeCacheSecondTripThread = null;
        if(computeCacheFirstTripThread != null && computeCacheFirstTripThread.getComputeCache() != null){
            computeCacheSecondTripThread = new ComputeCacheSecondTripThread(
                    computeCacheFirstTripThread.getComputeCache(), finalIntervals);
            computeCacheSecondTripThread.start();
        }

        List<SecondTripThread> pullThreads = new ArrayList<>(nodeNum);
        for(TwoTripsRPC.Client client : clients){
            SecondTripThread t = new SecondTripThread(client, buffer, recordSize);
            t.start();
            pullThreads.add(t);
        }

        for(SecondTripThread t : pullThreads){
            try{
                t.join();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        if(computeCacheSecondTripThread != null){
            try{
                computeCacheSecondTripThread.join();
            }catch (Exception e){
                e.printStackTrace();
            }
            if(computeCacheSecondTripThread.getError() != null){
                throw computeCacheSecondTripThread.getError();
            }
            System.out.println("compute cache two-trips final event size: "
                    + computeCacheSecondTripThread.getEvents().size());
        }
        for(SecondTripThread t : pullThreads){
            if(t.getError() != null){
                throw t.getError();
            }
        }
        logParallelPhase("two-trips-second",
                System.currentTimeMillis() - secondTripStartTime,
                computeCacheSecondTripThread,
                pullThreads);

        List<byte[]> filteredEvent = new ArrayList<>(4096);
        if(computeCacheSecondTripThread != null){
            filteredEvent.addAll(computeCacheSecondTripThread.getEvents());
        }
        for(SecondTripThread t : pullThreads){
            transmissionSize += t.getCommunicationCost();
            filteredEvent.addAll(t.getEvents());
        }
        System.out.println("transmission cost: " + transmissionSize + " bytes");
        return filteredEvent;
    }

    public static List<byte[]> communicate(String sql, List<TwoTripsRPC.Client> clients){
        return communicate(sql, clients, null);
    }

    public static void runQueries(List<TwoTripsRPC.Client> clients, String datasetName, boolean isEsper,
                                  ComputeNodeCache computeCacheStore, int queryLimit){
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
        int queryCount = queryLimit > 0 ? Math.min(queryLimit, sqlList.size()) : sqlList.size();
        for(int i = 0; i < queryCount; i++){
            System.out.println("query id: " + i);
            long queryStart = System.currentTimeMillis();
            String sql = sqlList.get(i);

            long startTime = System.currentTimeMillis();
            List<byte[]> byteRecords = communicate(sql, clients, computeCacheStore);
            long endTime = System.currentTimeMillis();
            System.out.println("final event size: " + byteRecords.size());

            // we need sort operation
            byteRecords = SortByTs.sort(byteRecords, eventSchema);
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

    public static void runQueries(List<TwoTripsRPC.Client> clients, String datasetName, boolean isEsper){
        runQueries(clients, datasetName, isEsper, null, -1);
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
        computeCacheScanThreadNum = intArg(args,
                new String[]{"--cache-scan-threads", "cacheScanThreads", "--compute-cache-scan-threads", "computeCacheScanThreads"},
                FullScan.threadNum * Math.max(1, nodeNum));
        int queryLimit = intArg(args, new String[]{"--query-limit", "queryLimit"}, -1);
        int timeout = 0;
        List<TTransport> transports = new ArrayList<>(nodeNum);
        List<TwoTripsRPC.Client> clients = new ArrayList<>(nodeNum);

        for(int i = 0; i < nodeNum; i++) {
            TSocket socket = new TSocket(conf, storageNodeIps[i], ports[i], timeout);
            TTransport transport = new TFramedTransport(socket, Args.maxMassageLen);
            TProtocol protocol = new TBinaryProtocol(transport);
            TwoTripsRPC.Client client = new TwoTripsRPC.Client(protocol);
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
                + " #computeCacheScanThreadNum: " + computeCacheScanThreadNum
                + " #queryLimit: " + queryLimit
                + " #filename: " + filename
                + " #isSaseEngine: " + Args.isSaseEngine);
        // System.out.println("@args #isEsper: " + isEsper + " #dataset: " + datasetName
        //         + " #storageNodeIps: " + String.join(",", storageNodeIps)
        //         + " #ports: " + portsToString(ports)
        //         + " #computeCachePageNum: " + Args.computeCachePageNum
        //         + " #queryLimit: " + queryLimit
        //         + " #filename: " + filename
        //         + " #isSaseEngine: " + Args.isSaseEngine);

        long cacheLoadStart = System.currentTimeMillis();
        ComputeNodeCache computeCacheStore = ComputeNodeCache.loadForTwoTrips(datasetName, clients, Args.computeCachePageNum);
        long cacheLoadEnd = System.currentTimeMillis();
        if(computeCacheStore != null){
            System.out.println("two-trips compute cache load cost: " + (cacheLoadEnd - cacheLoadStart)
                    + "ms, bytes: " + computeCacheStore.getCachedBytes());
        }

        LocalDateTime now = LocalDateTime.now();
        System.out.println("Start time " + now);
        long start = System.currentTimeMillis();
        // please modify datasetName, isEsper to change running mode
        runQueries(clients, datasetName, isEsper, computeCacheStore, queryLimit);
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
