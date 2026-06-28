package compute;

import engine.SelectionStrategy;
import org.apache.thrift.TConfiguration;
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

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RunPullAll {

    static class PullAllThread extends Thread{
        private final PushDownRPC.Client client;
        private final String tableName;

        private final int recordSize;
        public List<byte[]> events;
        private long communicationCost;

        public PullAllThread(PushDownRPC.Client client, String tableName, int recordSize){
            this.client = client;
            this.tableName = tableName;
            this.recordSize = recordSize;
            events = new ArrayList<>(2 * 1024 * 1024);
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public List<byte[]> getEvents(){
            return events;
        }

        @Override
        public void run(){
            try{
                boolean isLastChunk;
                int offset = 0;
                do{
                    DataChunk chunk = client.pullEvents(tableName, offset, null);
                    communicationCost += tableName.length() + 4;
                    communicationCost += chunk.data.capacity();
                    int recordNum = chunk.data.remaining() / recordSize;
                    for(int i = 0; i < recordNum; i++){
                        byte[] record = new byte[recordSize];
                        chunk.data.get(record);
                        events.add(record);
                    }
                    isLastChunk = chunk.isLastChunk;
                    offset += recordNum;
                }while (!isLastChunk);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static List<byte[]> communicate(String sql, List<PushDownRPC.Client> clients){
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        long transmissionSize = 0;
        Map<String, List<String>> ipMap = new HashMap<>();

        int nodeNum = clients.size();
        EventSchema schema = EventSchema.getEventSchema(tableName);
        int recordSize = schema.getFixedRecordLen();

        List<PullAllThread> threads = new ArrayList<>(nodeNum);
        for(PushDownRPC.Client client : clients){
            PullAllThread thread = new PullAllThread(client, tableName, recordSize);
            threads.add(thread);
            thread.start();
        }
        for(PullAllThread thread : threads){
            try{
                thread.join();
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }

        List<byte[]> allEvents = null;
        for(PullAllThread thread : threads){
            transmissionSize += thread.getCommunicationCost();
            if(allEvents == null){
                allEvents = thread.getEvents();
            }else{
                allEvents.addAll(thread.getEvents());
            }
        }

        System.out.println("transmission cost: " + transmissionSize + " bytes");
        return allEvents;
    }

    // here we only use sase engine
    public static void runQueries(List<PushDownRPC.Client> clients, String datasetName){
        EventSchema schema = EventSchema.getEventSchema(datasetName);

        List<String> sqlList = ReadQueries.getStrictConQueryList(datasetName);
        for(int i = 0; i < sqlList.size(); i++) {
            System.out.println("query id: " + i);
            long queryStart = System.currentTimeMillis();
            String sql = sqlList.get(i);
            long startTime = System.currentTimeMillis();
            List<byte[]> byteRecords = communicate(sql, clients);

            // if the query can be filtered first, we can apply filter here
            // byteRecords = filter(byteRecords, sql, schema);

            long endTime = System.currentTimeMillis();
            System.out.println("final event size: " + byteRecords.size());
            // we need sort operation
            long sortStart = System.currentTimeMillis();
            byteRecords = SortByTs.sort(byteRecords, schema);
            long sortEnd = System.currentTimeMillis();
            System.out.println("sort time: " + (sortEnd - sortStart) + "ms");
            System.out.println("pull event time: " + (endTime - startTime) + "ms");
            EvaluationEngineSase.processQuery(byteRecords, sql, SelectionStrategy.STRICT_CONTIGUOUS);
            long queryEnd = System.currentTimeMillis();
            System.out.println("this query cost: " + (queryEnd - queryStart) + "ms");
        }
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

    public static void main(String[] args) throws Exception{
        TConfiguration conf = new TConfiguration(Args.maxMassageLen, Args.maxMassageLen, Args.recursionLimit);
        String[] storageNodeIps = {"localhost"};
        int[] ports = {9090, 9090};
        int nodeNum = storageNodeIps.length;
        int timeout = 0;
        List<TTransport> transports = new ArrayList<>(nodeNum);
        List<PushDownRPC.Client> clients = new ArrayList<>(nodeNum);

        for(int i = 0; i < nodeNum; i++){
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
        System.out.println("@args #datasetName: " + datasetName + " #method: pull-all");
        LocalDateTime now = LocalDateTime.now();

        System.out.println("Start time " + now);
        long start = System.currentTimeMillis();
        // please modify datasetName, isEsper to change running mode
        runQueries(clients, datasetName);
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
