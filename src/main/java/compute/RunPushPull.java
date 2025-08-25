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
import utils.SortByTs;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class RunPushPull {
    static class InitialThread extends Thread {
        private final PushPullRPC.Client client;
        private final String tableName;
        private final String sql;
        private Map<String, Integer> varEventNumMap;
        private int communicatedByteSize;

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
        public void run() {
            try {
                varEventNumMap = client.initial(tableName, sql);
                communicatedByteSize += varEventNumMap.toString().length();
            }catch (Exception e) {e.printStackTrace();}
        }
    }

    // directly pull events without matching
    static class SimplePullThread extends Thread {
        private final PushPullRPC.Client client;
        private final List<String> requestedVarNames;
        private final int recordSize;

        private Map<String, List<byte[]>> pulledEventsMap;
        private int communicationCost;
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

        @Override
        public void run() {
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
                e.printStackTrace();
            }
        }
    }

    // pull events with matching
    static class ComplexPullThread extends Thread{
        private final PushPullRPC.Client client;
        private final List<String> requestedVarNames;

        private final Map<String, List<byte[]>> eventListMap;
        private Map<String, List<byte[]>> pulledEvents;

        private int communicationCost;
        private final int recordSize;

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

        @Override
        public void run() {
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
                        int curSize = entry.getValue().size();
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
                    sendOffset += maxReadOffset;

                    isLastChunk = maxReadOffset == -1;
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
                e.printStackTrace();
            }
        }
    }

    public static List<byte[]> communicate(String sql, List<PushPullRPC.Client> clients){
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        EventSchema schema = EventSchema.getEventSchema(tableName);
        int recordSize = schema.getFixedRecordLen();

        long transmissionSize = 0;

        int nodeNum = clients.size();
        // step 1: initial (process independent predicates and return cardinality)
        List<InitialThread> initialThreads = new ArrayList<>(nodeNum);
        for(PushPullRPC.Client client : clients){
            InitialThread thread = new InitialThread(client, tableName, sql);
            thread.start();
            initialThreads.add(thread);
        }
        for(InitialThread thread : initialThreads){
            try { thread.join(); }catch (Exception e) { e.printStackTrace(); }
        }
        // aggregate number of events
        Map<String, Integer> varEventNumMap = new HashMap<>();
        for(InitialThread thread : initialThreads){
            transmissionSize += thread.getCommunicatedByteSize();
            Map<String, Integer> map = thread.getVarEventNumMap();
            for(Map.Entry<String, Integer> entry : map.entrySet()){
                String key = entry.getKey();
                int value = entry.getValue();
                varEventNumMap.put(key, varEventNumMap.getOrDefault(key, 0) + value);
            }
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

                // union all events
                for (SimplePullThread thread : threads) {
                    transmissionSize += thread.getCommunicatedByteSize();
                    Map<String, List<byte[]>> pulledEvents = thread.getPulledEventsMap();
                    for (Map.Entry<String, List<byte[]>> entry : pulledEvents.entrySet()) {
                        String key = entry.getKey();
                        List<byte[]> list = entry.getValue();
                        eventMap.put(key, merge(eventMap.get(key), list, schema));
                    }
                }
            }
            else{
                List<byte[]> varEventList = eventMap.get(lastVarName);
                Map<String, List<byte[]>> sendMap = new HashMap<>();
                sendMap.put(lastVarName, varEventList);
                List<ComplexPullThread> threads = new ArrayList<>(nodeNum);
                for(PushPullRPC.Client client : clients){
                    ComplexPullThread thread = new ComplexPullThread(client, requestedVarNames, sendMap, recordSize);
                    thread.start();
                    threads.add(thread);
                }
                for(ComplexPullThread thread : threads){
                    try { thread.join(); }catch (Exception e) { e.printStackTrace(); }
                }

                for(ComplexPullThread thread : threads) {
                    transmissionSize += thread.getCommunicatedByteSize();
                    Map<String, List<byte[]>> pulledEvents = thread.getPulledEventsMap();
                    for(Map.Entry<String, List<byte[]>> entry : pulledEvents.entrySet()){
                        String key = entry.getKey();
                        List<byte[]> list = entry.getValue();
                        eventMap.put(key, merge(eventMap.get(key), list, schema));
                        // System.out.println("varName-> " + key + " event size: " + list.size());
                    }
                }
            }
            hasProcessedVarName.add(varName);
            lastVarName = varName;
        }

        List<byte[]> filteredEvents = null;
        for(List<byte[]> entry : eventMap.values()){
            filteredEvents = merge(filteredEvents, entry, schema);
        }
        System.out.println("transmission cost: " + transmissionSize + " bytes");
        return filteredEvents;
    }

    public static void runQueries(List<PushPullRPC.Client> clients, String datasetName, boolean isEsper){
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
        for(int i = 260; i < sqlList.size(); i++){
            System.out.println("query id: " + i);
            String sql = sqlList.get(i);
            // System.out.println(sql);
            long startTime = System.currentTimeMillis();
            List<byte[]> byteRecords = communicate(sql, clients);
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
        }
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

        // please modify two lines
        String[] storageNodeIps = {"localhost"};//, "172.27.146.110"
        int[] ports = {9090, 9090};

        int nodeNum = storageNodeIps.length;
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

//        String sep = File.separator;
//        String filePath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep + "output" + sep + "pushpull_cluster_sase.txt";
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
