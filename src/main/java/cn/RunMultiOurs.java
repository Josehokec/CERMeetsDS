package cn;

import compute.Args;
import compute.Estimator;
import filter.OptimizedSWF;
import filter.UpdatedMarkers;
import compute.EvaluationEngineSase;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.thrift.protocol.TBinaryProtocol;
import parser.EqualDependentPredicate;
import utils.Pair;
import parser.QueryParse;
import plan.GeneratedPlan;
import plan.Plan;
import request.ReadQueries;
import rpc2.iface.DataChunk;
import rpc2.iface.FilterBasedRPC2;
import rpc2.iface.FilteredResult;
import store.EventSchema;
import utils.ReplayIntervals;
import utils.SortByTs;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;

public class RunMultiOurs {

    static byte[] orByteArrays(byte[] array1, byte[] array2) {
        int length = Math.min(array1.length, array2.length);
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = (byte) (array1[i] | array2[i]);
        }
        return result;
    }

    static class MultiUser extends Thread{
        private final String clientId;
        List<TTransport> transports;
        List<FilterBasedRPC2.Client> clients;
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
                    FilterBasedRPC2.Client client = new FilterBasedRPC2.Client(protocol);
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
        private final FilterBasedRPC2.Client client;
        private final String tableName;
        // ip: independent predicate
        private final Map<String, List<String>> ipMap;
        private Map<String, Integer> varEventNumMap;
        private long communicationCost;
        private final String clientId;

        public InitialThread(FilterBasedRPC2.Client client, String clientId, String tableName, Map<String, List<String>> ipMap){
            this.client = client;
            this.tableName = tableName;
            this.ipMap = ipMap;
            communicationCost = 0;
            this.clientId = clientId;
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public Map<String, Integer> getVarEventNumMap(){
            return varEventNumMap;
        }

        @Override
        public void run() {
            try {
                varEventNumMap = client.initial(clientId, tableName, ipMap);
                communicationCost += tableName.length() + ipMap.toString().length();
                communicationCost += varEventNumMap.toString().length();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class GenerateIntervalThread extends Thread{
        private final FilterBasedRPC2.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private ReplayIntervals ri;
        private long communicationCost;
        private final String clientId;

        public GenerateIntervalThread(FilterBasedRPC2.Client client, String clientId, String varName, long window, int headTailMarker){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            communicationCost = 0;
            this.clientId = clientId;
        }

        public ReplayIntervals getReplayIntervals(){
            return ri;
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        @Override
        public void run() {
            try {
                ByteBuffer buffer = client.getInitialIntervals(clientId, varName, window, headTailMarker);
                communicationCost += varName.length() + 8 + 4;
                communicationCost += buffer.capacity();
                ri = ReplayIntervals.deserialize(buffer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class WindowFilterThread extends Thread{
        private final FilterBasedRPC2.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final ByteBuffer swfBuffer;
        private final String clientId;

        private UpdatedMarkers updateMarkers;
        private int filteredNum;
        private long communicationCost;

        public WindowFilterThread(FilterBasedRPC2.Client client, String clientId, String varName, long window, int headTailMarker, ByteBuffer swfBuffer){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.swfBuffer = swfBuffer;
            communicationCost = 0;
            this.clientId = clientId;
        }

        public int getFilteredNum(){
            return filteredNum;
        }

        public UpdatedMarkers getUpdateMarkers(){
            return updateMarkers;
        }

        public long getCommunicationCost(){
            return communicationCost;
        }

        @Override
        public void run() {
            try{
                FilteredResult res  = client.windowFilter(clientId, varName, window, headTailMarker, swfBuffer);
                communicationCost +=  varName.length() + 8 + 4 + swfBuffer.capacity();
                communicationCost += res.updatedSWF.capacity();
                updateMarkers = UpdatedMarkers.deserialize(res.updatedSWF);
                filteredNum = res.filteredNum;
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class RequestBloomFilterThread extends Thread{
        private final FilterBasedRPC2.Client client;
        private final String varName;
        private final long window;
        private final Map<String, List<String>> sendDPMap;
        private final Map<String, Integer> eventNumMap;
        private final ByteBuffer swf;
        private final String clientId;

        private Map<String, ByteBuffer> bfBufferMap;
        private long communicationCost;

        public Map<String, ByteBuffer> getBfBufferMap(){
            return bfBufferMap;
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public RequestBloomFilterThread(FilterBasedRPC2.Client client, String clientId, String varName, long window,
                                        Map<String, List<String>> sendDPMap, Map<String, Integer> eventNumMap, ByteBuffer swf){
            this.client = client;
            this.clientId = clientId;
            this.varName = varName;
            this.window = window;
            this.sendDPMap = sendDPMap;
            this.eventNumMap = eventNumMap;
            this.swf = swf;
            this.communicationCost = 0;
        }

        @Override
        public void run() {
            try{
                bfBufferMap = client.getBF4EQJoin(clientId, varName, window, sendDPMap, eventNumMap, swf);
                communicationCost = varName.length() + 8 + sendDPMap.toString().length() + swf.capacity();
                for(ByteBuffer b : bfBufferMap.values()){
                    communicationCost += b.remaining();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class EqJoinFilterThread extends Thread{
        private final FilterBasedRPC2.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final Map<String, Boolean> previousOrNext;
        private final Map<String, List<String>> sendDPMap;
        private final Map<String, ByteBuffer> bfBufferMap;
        private final String clientId;

        private UpdatedMarkers updatedMarkers;
        private long communicationCost;

        private int filteredNum;

        public EqJoinFilterThread(FilterBasedRPC2.Client client, String clientId, String varName, long window, int headTailMarker,
                                  Map<String, Boolean> previousOrNext, Map<String, List<String>> sendDPMap, Map<String, ByteBuffer> bfBufferMap){
            this.client = client;
            this.clientId = clientId;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.previousOrNext = previousOrNext;
            this.sendDPMap = sendDPMap;
            this.bfBufferMap = bfBufferMap;

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

        public int getFilteredNum() {
            return filteredNum;
        }

        @Override
        public void run() {
            try{
                FilteredResult ans  = client.eqJoinFilter(clientId, varName, window, headTailMarker, previousOrNext, sendDPMap, bfBufferMap);
                communicationCost += ans.updatedSWF.capacity() + 4;
                filteredNum = ans.filteredNum;
                updatedMarkers = UpdatedMarkers.deserialize(ans.updatedSWF);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class PullEventThread extends Thread{
        private final FilterBasedRPC2.Client client;
        private final long window;
        private final ByteBuffer swfBuffer;
        private final int recordLen;
        private final String clientId;

        private List<byte[]> events;
        private long communicationCost;

        public PullEventThread(FilterBasedRPC2.Client client, String clientId, long window, ByteBuffer swfBuffer, int recordLen){
            this.client = client;
            this.clientId = clientId;
            this.window = window;
            this.swfBuffer = swfBuffer;
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
        public void run() {
            try{
                boolean isLastChunk = true;
                do {
                    DataChunk chunk;
                    if(isLastChunk){
                        chunk  = client.getAllFilteredEvents(clientId, window, swfBuffer);
                    }else{
                        chunk  = client.getAllFilteredEvents(clientId, window, null);
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
            }
        }
    }


    public static List<byte[]> communicate(String clientId, String sql, List<FilterBasedRPC2.Client> clients){
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

        // step 1: initial (process independent predicates and return cardinality)
        List<InitialThread> initialThreads = new ArrayList<>(nodeNum);
        for(FilterBasedRPC2.Client client : clients){
            InitialThread initialThread = new InitialThread(client, clientId, tableName, ipMap);
            initialThread.start();
            initialThreads.add(initialThread);
        }
        for (InitialThread t : initialThreads) {
            try{ t.join(); }catch (Exception e){ e.printStackTrace(); }
        }

        // aggregate number of events
        Map<String, Integer> varEventNumMap = new HashMap<>(ipMap.size() << 1);
        for(InitialThread t : initialThreads){
            transmissionSize += t.getCommunicationCost();
            Map<String, Integer> map = t.getVarEventNumMap();
            for(Map.Entry<String, Integer> entry : map.entrySet()){
                String key = entry.getKey();
                int value = entry.getValue();
                varEventNumMap.put(key, varEventNumMap.getOrDefault(key, 0) + value);
            }
        }
        System.out.println("varEventNumMap: " + varEventNumMap);
        OptimizedSWF optimizedSWF = null;
        UpdatedMarkers updatedMarkers = null;

        Plan plan  = GeneratedPlan.filterPlan(varEventNumMap, query.getHeadVarName(), query.getTailVarName(), query.getDpMap());

        List<String> steps = plan.getSteps();
        Set<String> hasProcessedVarName = new HashSet<>();
        long slicedBits = -1;

        for(String varName : steps){
            if(hasProcessedVarName.isEmpty()){
                // initialization...
                List<GenerateIntervalThread> generateIntervalThreads = new ArrayList<>(nodeNum);
                for(FilterBasedRPC2.Client client : clients){
                    GenerateIntervalThread thread = new GenerateIntervalThread(client, clientId, varName, window, query.headTailMarker(varName));
                    thread.start();
                    generateIntervalThreads.add(thread);
                }
                for (GenerateIntervalThread t : generateIntervalThreads) {
                    try{ t.join(); }catch (Exception e){ e.printStackTrace(); }
                }

                ReplayIntervals replayIntervals = null;
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
                int keyNum = replayIntervals.getKeyNumber(window);
                List<ReplayIntervals.TimeInterval> timeIntervals = replayIntervals.getIntervals();

                optimizedSWF = new OptimizedSWF.Builder(keyNum).build();
                for(ReplayIntervals.TimeInterval interval : timeIntervals) {
                    optimizedSWF.insert(interval.getStartTime(), interval.getEndTime(), window);
                }
                slicedBits = optimizedSWF.getSliceNum();

                hasProcessedVarName.add(varName);
                filteredEventNum.put(varName, new Pair<>(varEventNumMap.get(varName), slicedBits));
            }
            else{
                Map<String, List<String>> sendDPMap = new HashMap<>(8);
                Map<String, Integer> eventNumMap = new HashMap<>(8);
                Map<String, Boolean> previousOrNext = new HashMap<>(8);

                boolean hasDP = false;
                // here we check whether query has equal dependent predicates, and estimate key number
                for(String hasVisitVarName : hasProcessedVarName){
                    String key = hasVisitVarName.compareTo(varName) < 0 ? (hasVisitVarName + "-" + varName) : (varName + "-" + hasVisitVarName);
                    if(equalDPMap.containsKey(key)){
                        hasDP = true;
                        sendDPMap.put(hasVisitVarName, equalDPMap.get(key));
                        // then we need to estimate number of keys
                        Pair<Integer, Long> p = filteredEventNum.get(hasVisitVarName);
                        int filterKeyNum = optimizedSWF.getKeyNum();
                        // one window
                        double estimateEventNum = (p.getKey() * slicedBits + 0.0) / p.getValue();

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
                ByteBuffer swfBuffer = optimizedSWF.serialize();

                int updatedEventNum = 0;

                if(hasDP){
                    List<RequestBloomFilterThread> requestBloomFilterThreads = new ArrayList<>(nodeNum);
                    for (FilterBasedRPC2.Client client : clients) {
                        RequestBloomFilterThread thread = new RequestBloomFilterThread(client, clientId, varName, window, sendDPMap, eventNumMap, swfBuffer);
                        thread.start();
                        requestBloomFilterThreads.add(thread);
                    }
                    for (RequestBloomFilterThread t : requestBloomFilterThreads){
                        try{ t.join(); }catch (Exception e){ e.printStackTrace(); }
                    }
                    // merge all bloom filters
                    Map<String, byte[]> mergedBFBufferMap = new HashMap<>(8);
                    for (RequestBloomFilterThread t : requestBloomFilterThreads){
                        transmissionSize += t.getCommunicationCost();
                        Map<String, ByteBuffer> bfBufferMap = t.getBfBufferMap();
                        for(String key : bfBufferMap.keySet()){
                            ByteBuffer bfBuffer = bfBufferMap.get(key);
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
                    //System.out.println("bf transmissionSize: " + transmissionSize);
                    Map<String, ByteBuffer> bfBufferMap = new HashMap<>(8);
                    for(String key : mergedBFBufferMap.keySet()){
                        ByteBuffer bfBuffer = ByteBuffer.wrap(mergedBFBufferMap.get(key));
                        bfBufferMap.put(key, bfBuffer);
                    }

                    // join filtering
                    List<EqJoinFilterThread> eqJoinFilterThreads = new ArrayList<>(nodeNum);
                    for (FilterBasedRPC2.Client client : clients) {
                        EqJoinFilterThread thread = new EqJoinFilterThread(client, clientId, varName, window, headTailMarker, previousOrNext, sendDPMap, bfBufferMap);
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

                    updatedMarkers = null;
                    for (EqJoinFilterThread t : eqJoinFilterThreads) {
                        transmissionSize += t.getCommunicationCost();
                        updatedEventNum += t.getFilteredNum();
                        if(updatedMarkers == null){
                            updatedMarkers = t.getUpdatedMarkers();
                        }else{
                            updatedMarkers.merge(t.getUpdatedMarkers());
                        }
                    }
                }
                else{
                    // only use window condition
                    List<WindowFilterThread> windowFilterThreads = new ArrayList<>(nodeNum);

                    for (FilterBasedRPC2.Client client : clients) {
                        WindowFilterThread thread = new WindowFilterThread(client, clientId, varName, window, query.headTailMarker(varName), swfBuffer);
                        thread.start();
                        windowFilterThreads.add(thread);
                    }
                    for (WindowFilterThread t : windowFilterThreads) {
                        try{
                            t.join();
                        }catch (Exception e){e.printStackTrace();}
                    }

                    updatedMarkers = null;
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
                optimizedSWF.rebuild(updatedMarkers);
                slicedBits = optimizedSWF.getSliceNum();


                filteredEventNum.put(varName, new Pair<>(updatedEventNum, slicedBits));
                hasProcessedVarName.add(varName);
            }
        }

        int recordLen = schema.getFixedRecordLen();
        ByteBuffer swfBuffer = optimizedSWF.serialize();

        List<PullEventThread> pullEventThreads = new ArrayList<>(nodeNum);
        for (FilterBasedRPC2.Client client : clients) {
            PullEventThread thread = new PullEventThread(client, clientId, window, swfBuffer, recordLen);
            thread.start();
            pullEventThreads.add(thread);
        }
        for (PullEventThread thread : pullEventThreads){
            try{ thread.join(); } catch (Exception e){ e.printStackTrace(); }
        }

        List<byte[]> byteRecords = null;
        for (PullEventThread thread : pullEventThreads){
            transmissionSize += thread.getCommunicationCost();
            if(byteRecords == null){
                byteRecords = thread.getAllEvents();
            }else{
                byteRecords.addAll(thread.getAllEvents());
            }
        }

        System.out.println(clientId + ", transmission cost: " + transmissionSize + " bytes");
        return byteRecords;
    }

    public static void runQueries(String clientId, List<String> sqlList, List<FilterBasedRPC2.Client> clients){
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
            // EvaluationEngineSase.processQuery(byteRecords, sql);
            try{
                Thread.sleep(1000); // wait for a while
            }catch (Exception e){
                e.printStackTrace();
            }
            long queryEnd = System.currentTimeMillis();
            System.out.println(clientId + ", " + i + "-th query cost: " + (queryEnd - queryStart) + "ms");
        }
    }

    public static void main(String[] args) throws Exception {
        LocalDateTime now = LocalDateTime.now();
        System.out.println("Start time " + now);

        int userNum = 1;
        String datasetName = "SYNTHETIC";
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
