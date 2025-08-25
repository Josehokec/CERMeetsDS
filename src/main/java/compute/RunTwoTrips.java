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
import utils.ReplayIntervals;
import utils.SortByTs;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class RunTwoTrips {

    static class FirstTripThread extends Thread{
        private final TwoTripsRPC.Client client;
        private final String tableName;
        private final String sql;

        private Map<String, ReplayIntervals> replayIntervalMap;
        private Map<String, Set<TsAttrPair>> tsAttrPairMap;
        private int communicationCost;

        public FirstTripThread(TwoTripsRPC.Client client, String tableName, String sql){
            this.client = client;
            this.tableName = tableName;
            this.sql = sql;

            replayIntervalMap = new HashMap<>(8);
            tsAttrPairMap = new HashMap<>(8);
        }

        public int getCommunicationCost() {
            return communicationCost;
        }

        public Map<String, ReplayIntervals> getReplayIntervalMap(){
            return replayIntervalMap;
        }

        public Map<String, Set<TsAttrPair>> getTsAttrPairMap(){
            return tsAttrPairMap;
        }

        @Override
        public void run(){
            try{
                boolean isLastChunk;
                int offset = 0;
                do{
                    TwoTripsDataChunk chunk = client.pullBasicInfo(tableName, sql, offset);
                    if(chunk.intervalMap != null){
                        for(String varName : chunk.intervalMap.keySet()){
                            ByteBuffer buffer = chunk.intervalMap.get(varName);
                            ReplayIntervals replayIntervals = ReplayIntervals.deserialize(buffer);
                            replayIntervalMap.put(varName, replayIntervals);
                            communicationCost += buffer.capacity() + varName.length();
                        }
                    }

                    int maxLen = 0;
                    if(!chunk.pairMap.isEmpty()){
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
                e.printStackTrace();
            }
        }
    }

    static class SecondTripThread extends Thread{
        private final TwoTripsRPC.Client client;
        private final ByteBuffer replayIntervalsBuffer;

        private final int recordSize;
        private List<byte[]> events;
        private long communicationCost;

        public SecondTripThread(TwoTripsRPC.Client client, ByteBuffer replayIntervalsBuffer, int recordSize){
            this.client = client;
            this.replayIntervalsBuffer = replayIntervalsBuffer;
            this.recordSize = recordSize;
            events = new ArrayList<>(4096);
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public List<byte[]> getEvents(){
            return events;
        }

        @Override
        public void run(){
            try {
                // we assume that without package loss, omit chunk id
                boolean isLastChunk;
                int offset = 0;
                do{
                    DataChunk chunk = client.pullEvents(offset, replayIntervalsBuffer);
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
                e.printStackTrace();
            }
        }
    }

    public static List<byte[]> communicate(String sql, List<TwoTripsRPC.Client> clients){
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        long transmissionSize = 0;

        int nodeNum = clients.size();
        EventSchema schema = EventSchema.getEventSchema(tableName);
        int recordSize = schema.getFixedRecordLen();

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

        //long time1Start = System.currentTimeMillis();
        // union replay intervals
        Map<String, ReplayIntervals> replayIntervalsMap = null;
        for(FirstTripThread t : firstTripThreads){
            transmissionSize += t.getCommunicationCost();
            if(replayIntervalsMap == null){
                replayIntervalsMap = t.getReplayIntervalMap();
            }else{
                // union
                Map<String, ReplayIntervals> unionMap = t.getReplayIntervalMap();
                for(String varName : unionMap.keySet()){
                    ReplayIntervals curIntervals = unionMap.get(varName);
                    if(replayIntervalsMap.get(varName) == null){
                        replayIntervalsMap.put(varName, curIntervals);
                    }else{
                        replayIntervalsMap.get(varName).union(curIntervals);
                    }
                }
            }
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
        Map<String, Set<TsAttrPair>> tsAttrPairMap = null;
        for(FirstTripThread t : firstTripThreads){
            if(tsAttrPairMap == null){
                tsAttrPairMap = t.getTsAttrPairMap();
            }else{
                // union
                Map<String, Set<TsAttrPair>> unionMap = t.getTsAttrPairMap();
                for(String varName : unionMap.keySet()){
                    Set<TsAttrPair> pairs = unionMap.get(varName);
                    if(tsAttrPairMap.get(varName) == null){
                        tsAttrPairMap.put(varName, pairs);
                    }else{
                        tsAttrPairMap.get(varName).addAll(pairs);
                    }
                }
            }
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
                        for(TsAttrPair pair : pairs){
                            long ts = pair.timestamp;
                            Map<Long, Set<String>> joinMap = windowAttrMap.get(key2);
                            if(finalIntervals.contains(ts)){
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

        List<byte[]> filteredEvent = null;
        for(SecondTripThread t : pullThreads){
            transmissionSize += t.getCommunicationCost();
            if(filteredEvent == null){
                filteredEvent = t.getEvents();
            }else{
                filteredEvent.addAll(t.getEvents());
            }
        }
        System.out.println("transmission cost: " + transmissionSize + " bytes");
        return filteredEvent;
    }

    public static void runQueries(List<TwoTripsRPC.Client> clients, String datasetName, boolean isEsper){
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
        for(int i = 0; i < sqlList.size(); i++){
            System.out.println("query id: " + i);
            String sql = sqlList.get(i);

            long startTime = System.currentTimeMillis();
            List<byte[]> byteRecords = communicate(sql, clients);
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
        }
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

    public static  void main(String[] args) throws Exception {
        TConfiguration conf = new TConfiguration(Args.maxMassageLen, Args.maxMassageLen, Args.recursionLimit);

        // please modify two lines
        String[] storageNodeIps = {"localhost"};//, "172.27.146.110"
        int[] ports = {9090, 9090};

        int nodeNum = storageNodeIps.length;
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

//        String sep = File.separator;
//        String filePath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep + "output" + sep + "two_trips_crimes_flink.txt";
//        System.setOut(new PrintStream(filePath));

        // "CRIMES", "CITIBIKE", "CLUSTER", "SYNTHETIC"
        String datasetName = "SYNTHETIC";
        boolean isEsper = false;
        System.out.println("@args #isEsper: " + isEsper + " #datasetName: " + datasetName + " #isSaseEngine: " + Args.isSaseEngine);
        LocalDateTime now = LocalDateTime.now();
        System.out.println("Start time " + now);
        long start = System.currentTimeMillis();
        // please modify datasetName, isEsper to change running mode
        runQueries(clients, datasetName, isEsper);
        long end = System.currentTimeMillis();

        now = LocalDateTime.now();
        System.out.println("Finish time " + now);
        System.out.println("Take " + (end - start) + "ms...");

        // we need to close transport
        for(TTransport transport : transports){
            transport.close();
        }
    }
}
