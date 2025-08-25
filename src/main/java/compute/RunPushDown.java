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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RunPushDown {

    static class PushDownThread extends Thread {
        private final PushDownRPC.Client client;
        private final String tableName;

        private final Map<String, List<String>> ipMap;
        private final int recordSize;
        private List<byte[]> events;
        private long communicationCost;

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

        @Override
        public void run(){
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
                e.printStackTrace();
            }
        }
    }

    public static List<byte[]> communicate(String sql, List<PushDownRPC.Client> clients){
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        long transmissionSize = 0;
        Map<String, List<String>> ipMap = query.getIpStringMap();

        int nodeNum = clients.size();
        EventSchema schema = EventSchema.getEventSchema(tableName);
        int recordSize = schema.getFixedRecordLen();

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

        List<byte[]> filteredEvent = null;
        for(PushDownThread t : pullThreads){
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

    public static void runQueries(List<PushDownRPC.Client> clients, String datasetName, boolean isEsper){
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


    public static  void main(String[] args) throws Exception {
        TConfiguration conf = new TConfiguration(Args.maxMassageLen, Args.maxMassageLen, Args.recursionLimit);
        String[] storageNodeIps = {"localhost"};//, "172.27.146.110"
        int[] ports = {9090, 9090};
        int nodeNum = storageNodeIps.length;
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

        //String sep = File.separator;
        //String filePath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep + "output" + sep + "pushdown_cluster_sase.txt";
        //System.setOut(new PrintStream(filePath));


        // "CRIMES", "CITIBIKE", "CLUSTER", "SYNTHETIC"
        String datasetName = "CRIMES";
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


/*
String testSql = "SELECT * FROM CLUSTER MATCH_RECOGNIZE(\n" +
                "    PARTITION BY JOBID\n" +
                "    ORDER BY eventTime\n" +
                "    MEASURES A.eventTime as ATS, B.eventTime as BTS, C.eventTime AS CTS\n" +
                "    ONE ROW PER MATCH\n" +
                "    AFTER MATCH SKIP TO NEXT ROW\n" +
                "    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '5' SECOND\n" +
                "    DEFINE\n" +
                "        A AS A.type = 'TP0' AND A.CPU <= 0.015 AND A.RAM <= 0.015,\n" +
                "        B AS B.type = 'TP1' AND B.JOBID = A.JOBID AND B.index < 5 AND B.CPU <= 0.015 AND B.RAM <= 0.015,\n" +
                "        C AS C.type = 'TP5' AND C.JOBID = B.JOBID AND C.CPU <= 0.015 AND C.RAM <= 0.015\n" +
                ");";
 */