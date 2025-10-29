package cn;

import compute.Args;
import compute.EvaluationEngineSase;
import plan.GeneratedPlan;
import plan.Plan;
import request.ReadQueries;
import rpc2.iface.PushPullDataChunk;
import rpc2.iface.PushPullRPC2;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import store.EventSchema;
import parser.QueryParse;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;

public class RunMultiPushPull {

    static class MultiUser extends Thread{
        private final String clientId;
        List<TTransport> transports;
        List<PushPullRPC2.Client> clients;
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
            int[] ports = {9090, 9090};
            int nodeNum = storageNodeIps.length;
            int timeout = 0;
            transports = new ArrayList<>(nodeNum);
            clients = new ArrayList<>(nodeNum);

            try{
                for(int i = 0; i < nodeNum; i++) {
                    TSocket socket = new TSocket(conf, storageNodeIps[i], ports[i], timeout);
                    TTransport transport = new TFramedTransport(socket, Args.maxMassageLen);
                    TProtocol protocol = new TBinaryProtocol(transport);
                    PushPullRPC2.Client client = new PushPullRPC2.Client(protocol);
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

    static class InitialThread extends Thread {
        private final PushPullRPC2.Client client;
        private final String tableName;
        private final String sql;
        private final String clientId;
        private Map<String, Integer> varEventNumMap;
        private int communicatedByteSize;

        public InitialThread(PushPullRPC2.Client client, String clientId, String tableName, String sql) {
            this.client = client;
            this.tableName = tableName;
            this.sql = sql;
            this.clientId = clientId;
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
                varEventNumMap = client.initial(clientId, tableName, sql);
                communicatedByteSize += varEventNumMap.toString().length();
            }catch (Exception e) {e.printStackTrace();}
        }
    }

    // directly pull events without matching
    static class SimplePullThread extends Thread {
        private final PushPullRPC2.Client client;
        private final List<String> requestedVarNames;
        private final int recordSize;
        private final String clientId;

        private Map<String, List<byte[]>> pulledEventsMap;
        private int communicationCost;
        public SimplePullThread(PushPullRPC2.Client client, String clientId, List<String> requestedVarNames, int recordSize) {
            this.client = client;
            this.requestedVarNames = requestedVarNames;
            this.clientId = clientId;
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
                    PushPullDataChunk chunk = client.getEventsByVarName(clientId, requestedVarNames, offset);
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
        private final PushPullRPC2.Client client;
        private final List<String> requestedVarNames;
        private final String clientId;
        private final Map<String, List<byte[]>> eventListMap;
        private Map<String, List<byte[]>> pulledEvents;

        private int communicationCost;
        private final int recordSize;

        private int sendOffset = 0;

        public ComplexPullThread(PushPullRPC2.Client client, String clientId, List<String> requestedVarNames, Map<String, List<byte[]>> eventListMap, int recordSize) {
            this.client = client;
            this.requestedVarNames = requestedVarNames;
            this.eventListMap = eventListMap;
            this.clientId = clientId;
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
                    chunk = client.matchFilter(clientId, requestedVarNames, sendChunk, offset);
                    communicationCost += 4;
                }while (!isLastChunk);

                // receive data...
                boolean firstEntrance = true;
                do{
                    // special case processing
                    if(firstEntrance){
                        firstEntrance = false;
                    }else{
                        chunk = client.matchFilter(clientId, requestedVarNames, new PushPullDataChunk(-1, null, true), offset);
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

    public static List<byte[]> communicate(String clientId, String sql, List<PushPullRPC2.Client> clients){
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        EventSchema schema = EventSchema.getEventSchema(tableName);
        int recordSize = schema.getFixedRecordLen();

        long transmissionSize = 0;

        int nodeNum = clients.size();
        // step 1: initial (process independent predicates and return cardinality)
        List<InitialThread> initialThreads = new ArrayList<>(nodeNum);
        for(PushPullRPC2.Client client : clients){
            InitialThread thread = new InitialThread(client, clientId, tableName, sql);
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
        // System.out.println("varEventNumMap: " + varEventNumMap);
        Map<String, List<byte[]>> eventMap = new HashMap<>();

        // here we should choose
        query.getDpMap();
        Plan plan  = GeneratedPlan.pullPushPlan(varEventNumMap, query.getDpMap());
        List<String> steps = plan.getSteps();
        // System.out.println("steps: " + steps);
        Set<String> hasProcessedVarName = new HashSet<>();
        String lastVarName = "";
        for(String varName : steps){
            List<String> requestedVarNames = new ArrayList<>(4);
            requestedVarNames.add(varName);
            if(hasProcessedVarName.isEmpty()) {
                List<SimplePullThread> threads = new ArrayList<>(nodeNum);
                for (PushPullRPC2.Client client : clients) {
                    SimplePullThread thread = new SimplePullThread(client, clientId, requestedVarNames, recordSize);
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
                for(PushPullRPC2.Client client : clients){
                    ComplexPullThread thread = new ComplexPullThread(client, clientId, requestedVarNames, sendMap, recordSize);
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
        System.out.println(clientId + ", transmission cost: " + transmissionSize + " bytes");
        return filteredEvents;

    }

    public static void runQueries(String clientId, List<String> sqlList, List<PushPullRPC2.Client> clients){
        for(int i = 0; i < sqlList.size(); i++){
            System.out.println(clientId + ", query id: " + i);
            long queryStart = System.currentTimeMillis();
            String sql = sqlList.get(i);
            long startTime = System.currentTimeMillis();
            List<byte[]> byteRecords = communicate(clientId, sql, clients);
            long endTime = System.currentTimeMillis();
            System.out.println(clientId + ", final event size: " + byteRecords.size());

            System.out.println(clientId + ", pull event time: " + (endTime - startTime) + "ms");
            EvaluationEngineSase.processQuery(byteRecords, sql);

            long queryEnd = System.currentTimeMillis();
            System.out.println(clientId + ", " + i + "-th query cost: " + (queryEnd - queryStart) + "ms");
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
