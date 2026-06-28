package cn;

import compute.Args;
import compute.EvaluationEngineSase;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.thrift.protocol.TBinaryProtocol;
import parser.QueryParse;
import request.ReadQueries;
import rpc2.iface.PushDownRPC2;
import rpc2.iface.DataChunk;
import store.EventSchema;
import utils.SortByTs;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RunMultiPushDown {

    static class MultiUser extends Thread{
        private final String clientId;
        List<TTransport> transports;
        List<PushDownRPC2.Client> clients;
        private final List<String> sqlList;

        public MultiUser(String clientId, String datasetName, int userNum, int userId){
            this.clientId = clientId;

            List<String> allQueries = ReadQueries.getQueryList(datasetName, false);
            int from = userId * (allQueries.size() / userNum);
            int to = (userId + 1) * (allQueries.size() / userNum);
            if(userId == userNum -1){
                to = allQueries.size();
            }
            sqlList = allQueries.subList(from, to);
            System.out.println("sqlList size: " + sqlList.size());

            TConfiguration conf = new TConfiguration(Args.maxMassageLen, Args.maxMassageLen, Args.recursionLimit);
            // please modify according to your storage node IPs and ports
            String[] storageNodeIps = {"localhost"};
            int[] ports = {9090};
            int nodeNum = storageNodeIps.length;
            int timeout = 0;
            transports = new ArrayList<>(nodeNum);
            clients = new ArrayList<>(nodeNum);

            try{
                for(int i = 0; i < nodeNum; i++) {
                    TSocket socket = new TSocket(conf, storageNodeIps[i], ports[i], timeout);
                    TTransport transport = new TFramedTransport(socket, Args.maxMassageLen);
                    TProtocol protocol = new TBinaryProtocol(transport);
                    PushDownRPC2.Client client = new PushDownRPC2.Client(protocol);
                    // when we open, we can call related interface
                    transport.open();
                    clients.add(client);
                    transports.add(transport);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        @Override
        public void run() {

            runQueries(clientId, sqlList, clients);

            // we need to close transport
            for(TTransport transport : transports){
                transport.close();
            }
        }
    }

    static class PushDownThread extends Thread {
        private final PushDownRPC2.Client client;
        private final String clientId;
        private final String tableName;

        private final Map<String, List<String>> ipMap;
        private final int recordSize;
        private List<byte[]> events;
        private long communicationCost;

        public PushDownThread(PushDownRPC2.Client client, String clientId, String tableName,
                              Map<String, List<String>> ipMap, int recordSize) {
            this.client = client;
            this.clientId = clientId;
            this.tableName = tableName;
            this.ipMap = ipMap;
            this.recordSize = recordSize;
            this.communicationCost = 0L;
            events = new ArrayList<>(4096);
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public List<byte[]> getEvents() {
            return events;
        }

        @Override
        public void run() {
            try {
                // we assume that without package loss, omit chunk id
                boolean isLastChunk;
                int offset = 0;
                do{
                    DataChunk chunk = client.pullEvents(clientId, tableName, offset, ipMap);
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

    public static List<byte[]> communicate(String clientId, String sql, List<PushDownRPC2.Client> clients){
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        long transmissionSize = 0;
        Map<String, List<String>> ipMap = query.getIpStringMap();

        int nodeNum = clients.size();
        EventSchema schema = EventSchema.getEventSchema(tableName);
        int recordSize = schema.getFixedRecordLen();

        List<PushDownThread> pullThreads = new ArrayList<>(nodeNum);
        for(PushDownRPC2.Client client : clients){
            PushDownThread thread = new PushDownThread(client, clientId, tableName, ipMap, recordSize);
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

    public static void runQueries(String clientId, List<String> sqlList, List<PushDownRPC2.Client> clients){
        for(int i = 0; i < sqlList.size(); i++){
            System.out.println(clientId + ", query id: " + i);
            long queryStart = System.currentTimeMillis();
            String sql = sqlList.get(i);
            long startTime = System.currentTimeMillis();
            List<byte[]> byteRecords = communicate(clientId, sql, clients);
            long endTime = System.currentTimeMillis();
            System.out.println(clientId + ", final event size: " + byteRecords.size());

            // we need sort operation
            QueryParse queryParser = new QueryParse(sql);
            String datasetName = queryParser.getTableName();
            EventSchema eventSchema = EventSchema.getEventSchema(datasetName);

            byteRecords = SortByTs.sort(byteRecords, eventSchema);
            System.out.println(clientId + ", pull event time: " + (endTime - startTime) + "ms");
            EvaluationEngineSase.processQuery(byteRecords, sql);

            long queryEnd = System.currentTimeMillis();
            System.out.println(clientId + ", " + i + "-th query cost: " + (queryEnd - queryStart) + "ms");
        }
    }

    public static void main(String[] args) throws Exception {
        LocalDateTime now = LocalDateTime.now();
        System.out.println("Start time " + now);

        int userNum = 5;
        String datasetName = "CRIMES";
        String computeName = "CN0";//CN0, CN1, ...
        List<MultiUser> users = new ArrayList<>(userNum);

        for(int i = 0; i < userNum; i++){
            MultiUser user = new MultiUser(computeName + "-USER" + i, datasetName, userNum, i);
            user.start();
            users.add(user);
        }
        for(MultiUser user : users){
            user.join();
        }

        now = LocalDateTime.now();
        System.out.println("Finish time " + now);
    }

}
