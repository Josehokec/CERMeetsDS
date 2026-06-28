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
import parser.IndependentPredicate;
import parser.QueryParse;
import request.ReadQueries;
import rpc.iface.DataChunk;
import rpc.iface.PushDownRPC;
import store.ColumnInfo;
import store.EventSchema;
import store.FullScan;
import utils.Pair;
import utils.SortByTs;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class RunPushDown {
    private static final int DEFAULT_CACHE_PAGE_NUM = 8192;

    static class PushDownThread extends Thread {
        private final PushDownRPC.Client client;
        private final String tableName;

        private final Map<String, List<String>> ipMap;
        private final int recordSize;
        private List<byte[]> events;
        private long communicationCost;
        private long elapsedTime;
        private RuntimeException error;

        public PushDownThread(PushDownRPC.Client client, String tableName, Map<String, List<String>> ipMap,  int recordSize){
            this.client = client;
            this.tableName = tableName;
            this.ipMap = ipMap;
            this.recordSize = recordSize;
            events = new ArrayList<>(4096);
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public List<byte[]> getEvents(){
            return events;
        }

        public long getElapsedTime() {
            return elapsedTime;
        }

        public RuntimeException getError() {
            return error;
        }

        @Override
        public void run(){
            long startTime = System.currentTimeMillis();
            try {
                // we assume that without package loss, omit chunk id
                boolean isLastChunk;
                int offset = 0;
                do{
                    DataChunk chunk = client.pullEvents(tableName, offset, ipMap);
                    communicationCost += tableName.length() + 4 + ipMap.toString().length();
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
                error = new RuntimeException("Failed to pull push-down events from storage node", e);
            } finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    static class ComputeCachePushDownThread extends Thread {
        private final ComputeNodeCache computeCacheStore;
        private final Map<String, List<String>> ipMap;
        private final int cacheThreadNum;
        private List<byte[]> events;
        private long elapsedTime;
        private RuntimeException error;

        ComputeCachePushDownThread(ComputeNodeCache computeCacheStore, Map<String, List<String>> ipMap,
                                   int cacheThreadNum) {
            this.computeCacheStore = computeCacheStore;
            this.ipMap = ipMap;
            this.cacheThreadNum = cacheThreadNum;
            this.events = new ArrayList<>(0);
        }

        public List<byte[]> getEvents() {
            return events;
        }

        public int getEventNum() {
            return events.size();
        }

        public long getElapsedTime() {
            return elapsedTime;
        }

        public RuntimeException getError() {
            return error;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try{
                List<List<byte[]>> eventGroups =
                        computeCacheStore.filterForPushDownByNodeInParallel(ipMap, cacheThreadNum);
                int eventNum = 0;
                for(List<byte[]> eventGroup : eventGroups){
                    eventNum += eventGroup.size();
                }
                List<byte[]> filteredEvents = new ArrayList<>(eventNum);
                for(List<byte[]> eventGroup : eventGroups){
                    filteredEvents.addAll(eventGroup);
                }
                events = filteredEvents;
            }catch (Exception e){
                error = new RuntimeException("Failed to filter push-down compute cache", e);
            }finally {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
        }
    }

    public static List<byte[]> communicate(String sql, List<PushDownRPC.Client> clients,
                                           ComputeNodeCache computeCacheStore, int cacheThreadNum){
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        long transmissionSize = 0;
        Map<String, List<String>> ipMap = copyIpMap(query.getIpStringMap());

        int nodeNum = clients.size();
        EventSchema schema = EventSchema.getEventSchema(tableName);
        int recordSize = schema.getFixedRecordLen();

        long pushDownStart = System.currentTimeMillis();
        ComputeCachePushDownThread computeCacheThread = null;
        if(computeCacheStore != null){
            computeCacheThread = new ComputeCachePushDownThread(computeCacheStore, ipMap, cacheThreadNum);
            computeCacheThread.start();
        }

        List<PushDownThread> pullThreads = new ArrayList<>(nodeNum);

        for(PushDownRPC.Client client : clients){
            PushDownThread thread = new PushDownThread(client, tableName, ipMap, recordSize);
            thread.start();
            pullThreads.add(thread);
        }
        for (PushDownThread t : pullThreads){
            try{
                t.join();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        if(computeCacheThread != null){
            try{
                computeCacheThread.join();
            }catch (Exception e){
                e.printStackTrace();
            }
            if(computeCacheThread.getError() != null){
                throw computeCacheThread.getError();
            }
        }

        List<byte[]> filteredEvent = new ArrayList<>(4096);
        if(computeCacheThread != null){
            System.out.println("compute cache push-down event size: " + computeCacheThread.getEventNum());
            System.out.println("compute cache push-down filter cost: " + computeCacheThread.getElapsedTime()
                    + "ms, threads=" + cacheThreadNum);
        }
        long storageMaxTime = 0;
        if(computeCacheThread != null){
            filteredEvent.addAll(computeCacheThread.getEvents());
        }
        for(int i = 0; i < pullThreads.size(); i++){
            PushDownThread t = pullThreads.get(i);
            if(t.getError() != null){
                throw t.getError();
            }
            transmissionSize += t.getCommunicationCost();
            storageMaxTime = Math.max(storageMaxTime, t.getElapsedTime());
            filteredEvent.addAll(t.getEvents());
        }

        long cacheTime = computeCacheThread == null ? 0 : computeCacheThread.getElapsedTime();
        System.out.println("push-down parallel phase wall="
                + (System.currentTimeMillis() - pushDownStart)
                + "ms, cache=" + cacheTime + "ms, storageMax=" + storageMaxTime + "ms");
        System.out.println("transmission cost: " + transmissionSize + " bytes");
        return filteredEvent;
    }

    public static List<byte[]> communicate(String sql, List<PushDownRPC.Client> clients, ComputeNodeCache computeCacheStore){
        return communicate(sql, clients, computeCacheStore, FullScan.threadNum);
    }

    public static List<byte[]> communicate(String sql, List<PushDownRPC.Client> clients){
        return communicate(sql, clients, null);
    }

    public static void runQueries(List<PushDownRPC.Client> clients, String datasetName, boolean isEsper,
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

        List<String> sqlList = ReadQueries.getQueryList(datasetName, false);
        List<String> esperSqlList = ReadQueries.getQueryList(datasetName, true);

        // sqlList.size()
        int queryCount = queryLimit > 0 ? Math.min(queryLimit, sqlList.size()) : sqlList.size();
        for(int i = 0; i < queryCount; i++){
            System.out.println("query id: " + i);
            long queryStart = System.currentTimeMillis();
            String sql = sqlList.get(i);
            long startTime = System.currentTimeMillis();
            List<byte[]> byteRecords = communicate(sql, clients, computeCacheStore, cacheThreadNum);
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

    public static void runQueries(List<PushDownRPC.Client> clients, String datasetName, boolean isEsper,
                                  ComputeNodeCache computeCacheStore, int queryLimit){
        runQueries(clients, datasetName, isEsper, computeCacheStore, queryLimit, FullScan.threadNum);
    }

    public static void runQueries(List<PushDownRPC.Client> clients, String datasetName, boolean isEsper){
        runQueries(clients, datasetName, isEsper, null, -1);
    }

    public static Schema getSchemaByDatasetName(String datasetName){
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

    public static List<byte[]> filter(List<byte[]> records, String sql, EventSchema schema){
        List<byte[]> filteredRecords = new ArrayList<>(8 * 1024);
        QueryParse query = new QueryParse(sql);
        Map<String, List<String>> ipMap = query.getIpStringMap();
        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> newIpMap = FullScan.parseIpString(ipMap, schema);
        for (byte[] record : records){
            boolean satisfy = true;
            for(Map.Entry<String, List<Pair<IndependentPredicate, ColumnInfo>>> entry : newIpMap.entrySet()){
                for(Pair<IndependentPredicate, ColumnInfo> ipPair : entry.getValue()){
                    IndependentPredicate ip = ipPair.getKey();
                    ColumnInfo columnInfo = ipPair.getValue();
                    ByteBuffer bb = ByteBuffer.wrap(record);
                    Object obj = schema.getColumnValue(columnInfo, bb, 0);
                    satisfy = ip.check(obj, columnInfo.getDataType());
                    if(!satisfy){
                        break;
                    }
                }
                if(satisfy){
                    filteredRecords.add(record);
                    break;
                }
            }
        }
        return filteredRecords;
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
        int cacheThreadNum = intArg(args, new String[]{"--cache-threads", "cacheThreads"}, FullScan.threadNum);
        int timeout = 0;
        List<TTransport> transports = new ArrayList<>(nodeNum);
        List<PushDownRPC.Client> clients = new ArrayList<>(nodeNum);

        for(int i = 0; i < nodeNum; i++) {
            TSocket socket = new TSocket(conf, storageNodeIps[i], ports[i], timeout);
            TTransport transport = new TFramedTransport(socket, Args.maxMassageLen);
            TProtocol protocol = new TBinaryProtocol(transport);
            PushDownRPC.Client client = new PushDownRPC.Client(protocol);
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
                + " #filename: " + filename
                + " #isSaseEngine: " + Args.isSaseEngine);

        long cacheLoadStart = System.currentTimeMillis();
        ComputeNodeCache computeCacheStore = ComputeNodeCache.loadForPushDown(datasetName, clients, Args.computeCachePageNum);
        long cacheLoadEnd = System.currentTimeMillis();
        if(computeCacheStore != null){
            System.out.println("push-down compute cache load cost: " + (cacheLoadEnd - cacheLoadStart)
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

    private static Map<String, List<String>> copyIpMap(Map<String, List<String>> ipMap) {
        Map<String, List<String>> copied = new HashMap<>(ipMap.size() << 1);
        for(Map.Entry<String, List<String>> entry : ipMap.entrySet()){
            copied.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        return copied;
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

//    public static String generateClientId() {
//        String clientId = generateClientId();
//        String clientIdAndTableName = clientId + "::" + tableName;
//        System.out.println("client id: " +  clientId);
//
//        String hostname = "CN_00";
//        return hostname + UUID.randomUUID().toString().substring(0, 32);
//    }
