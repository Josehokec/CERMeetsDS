package compute;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import parser.QueryParse;
import plan.GeneratedPlan;
import plan.Plan;
import request.ReadQueries;
import rpc.iface.BasicFilterBasedRPC;
import rpc.iface.SDataChunk;
import store.EventSchema;
import utils.Pair;
import utils.ReplayIntervals;
import utils.SortByTs;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;

// Multi-trips approach implementation
public class RunNaiveMultiTrips {

    static class InitialThread extends Thread{
        private final BasicFilterBasedRPC.Client client;
        private final String tableName;
        private final Map<String, List<String>> ipMap;
        private Map<String, Integer> varEventNumMap;
        private long communicationCost;

        public InitialThread(BasicFilterBasedRPC.Client client, String tableName, Map<String, List<String>> ipMap){
            this.client = client;
            this.tableName = tableName;
            this.ipMap = ipMap;
            communicationCost = 0;
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
                varEventNumMap = client.initial(tableName, ipMap);
                communicationCost += tableName.length() + ipMap.toString().length();
                communicationCost += varEventNumMap.toString().length();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class GenerateIntervalThread extends Thread{
        private final BasicFilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private ReplayIntervals ri;
        private long communicationCost;

        public GenerateIntervalThread(BasicFilterBasedRPC.Client client, String varName, long window, int headTailMarker){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            communicationCost = 0;
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
                ByteBuffer buffer = client.getInitialIntervals(varName, window, headTailMarker);
                communicationCost += varName.length() + 8 + 4;
                communicationCost += buffer.capacity();
                ri = ReplayIntervals.deserialize(buffer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class WindowFilterThread extends Thread{
        private final BasicFilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final ByteBuffer intervals;

        private ReplayIntervals updatedIntervals;
        private long communicationCost;

        public WindowFilterThread(BasicFilterBasedRPC.Client client, String varName, long window, int headTailMarker, ByteBuffer intervals){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.intervals = intervals;
            communicationCost = 0;
        }

        public ReplayIntervals getReplayIntervals(){
            return updatedIntervals;
        }

        public long getCommunicationCost(){
            return communicationCost;
        }

        @Override
        public void run() {
            try{
                ByteBuffer intervalBuff  = client.windowFilter(varName, window, headTailMarker, intervals);
                communicationCost +=  varName.length() + 8 + 4 + intervals.capacity();
                communicationCost += intervalBuff.capacity();
                updatedIntervals = ReplayIntervals.deserialize(intervalBuff);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class RequestJoinInfoThread extends Thread{
        private final BasicFilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final Map<String, List<String>> sendDPMap;
        private final ByteBuffer intervals;
        private Map<String, Map<Long, Set<String>>> pairs;
        private long communicationCost;

        public Map<String, Map<Long, Set<String>>> getPairs(){
            return pairs;
        }

        public long getCommunicationCost(){
            return communicationCost;
        }

        public RequestJoinInfoThread(BasicFilterBasedRPC.Client client, String varName, long window, Map<String, List<String>> sendDPMap, ByteBuffer intervals){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.sendDPMap = sendDPMap;
            this.intervals = intervals;
            communicationCost = 0;
        }

        @Override
        public void run() {
            try{
                pairs = client.getHashTable4EQJoin(varName, window, sendDPMap, intervals);
                communicationCost += varName.length() + 8 + sendDPMap.toString().length() + intervals.capacity();
                communicationCost += pairs.toString().length();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class EqJoinFilterThread extends Thread{
        private final BasicFilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final Map<String, Boolean> previousOrNext;
        private final Map<String, List<String>> dpStrMap;
        private final Map<String, Map<Long, Set<String>>> pairs;
        private ByteBuffer updatedIntervals;
        private long communicationCost;

        public ByteBuffer getUpdatedIntervals(){
            return updatedIntervals;
        }

        public long getCommunicationCost(){
            return communicationCost;
        }

        public EqJoinFilterThread(BasicFilterBasedRPC.Client client, String varName, long window, int headTailMarker,
                                  Map<String, Boolean> previousOrNext, Map<String, List<String>> dpStrMap,
                                  Map<String, Map<Long, Set<String>>> pairs){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.previousOrNext = previousOrNext;
            this.dpStrMap = dpStrMap;
            this.pairs = pairs;
            communicationCost = 0;
        }

        @Override
        public void run() {
            try{
                updatedIntervals = client.eqJoinFilter(varName, window, headTailMarker, previousOrNext, dpStrMap, pairs);
                communicationCost += varName.length() + 8 + 4 + previousOrNext.toString().length()
                        + dpStrMap.toString().length() + pairs.toString().length();
                communicationCost += updatedIntervals.capacity();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class PullEventThread extends Thread{
        private final BasicFilterBasedRPC.Client client;
        private final ByteBuffer intervals;
        private final int recordLen;


        private List<byte[]> events;
        private long communicationCost;

        public List<byte[]> getEvents(){
            return events;
        }

        public long getCommunicationCost(){
            return communicationCost;
        }

        public PullEventThread(BasicFilterBasedRPC.Client client, ByteBuffer intervals, int recordLen){
            this.client = client;
            this.intervals = intervals;
            this.recordLen = recordLen;

            communicationCost = intervals.capacity();
        }

        @Override
        public void run() {
            try{
                boolean isLastChunk = true;
                do{
                    SDataChunk chunk;
                    if(isLastChunk){
                        chunk  = client.getAllFilteredEvents(-1, intervals);
                    }else{
                        chunk  = client.getAllFilteredEvents(-1, null);
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

    public static List<byte[]> communicate(String sql, List<BasicFilterBasedRPC.Client> clients){
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        Map<String, List<String>> ipMap = query.getIpStringMap();
        Map<String, List<String>> equalDPMap = query.getEqualDpMap();
        long window = query.getWindow();
        int nodeNum = clients.size();
        EventSchema schema = EventSchema.getEventSchema(tableName);

        long transmissionSize = 0;

        // step 1: initial (process independent predicates and return cardinality)
        List<InitialThread> initialThreads = new ArrayList<>(nodeNum);
        for(BasicFilterBasedRPC.Client client : clients){
            InitialThread thread = new InitialThread(client, tableName, ipMap);
            initialThreads.add(thread);
            thread.start();
        }
        for(InitialThread thread : initialThreads){
            try{ thread.join(); }catch (Exception e){ e.printStackTrace(); }
        }

        Map<String, Integer> varEventNumMap = new HashMap<>(ipMap.size() << 1);
        for(InitialThread thread : initialThreads){
            transmissionSize += thread.getCommunicationCost();
            Map<String, Integer> map = thread.getVarEventNumMap();
            for(Map.Entry<String, Integer> entry : map.entrySet()){
                String key = entry.getKey();
                int value = entry.getValue();
                varEventNumMap.put(key, varEventNumMap.getOrDefault(key, 0) + value);
            }
        }
        System.out.println("varEventNumMap: " + varEventNumMap);

        ReplayIntervals localIntervals = null;
        Plan plan  = GeneratedPlan.filterPlan(varEventNumMap, query.getHeadVarName(), query.getTailVarName(), query.getDpMap());
        List<String> steps = plan.getSteps();
        Set<String> hasProcessedVarName = new HashSet<>();

        for(String varName : steps){
            if(hasProcessedVarName.isEmpty()){
                // initialization...
                List<GenerateIntervalThread> generateIntervalThreads = new ArrayList<>();
                for(BasicFilterBasedRPC.Client client : clients) {
                    GenerateIntervalThread thread = new GenerateIntervalThread(client, varName, window, query.headTailMarker(varName));
                    thread.start();
                    generateIntervalThreads.add(thread);
                }
                for(GenerateIntervalThread thread : generateIntervalThreads){
                    try{ thread.join(); }catch (Exception e){ e.printStackTrace(); }
                }

                for(GenerateIntervalThread thread : generateIntervalThreads){
                    transmissionSize += thread.getCommunicationCost();
                    ReplayIntervals ri = thread.getReplayIntervals();
                    if(localIntervals == null){
                        localIntervals = ri;
                    }else{
                        localIntervals.union(ri);
                    }
                }

                hasProcessedVarName.add(varName);
            }
            else{
                Map<String, List<String>> sendDPMap = new HashMap<>(8);
                Map<String, Boolean> previousOrNext = new HashMap<>(8);

                boolean hasDP = false;
                // here we check whether query has equal dependent predicates, and estimate key number
                for(String hasVisitVarName : hasProcessedVarName) {
                    String key = hasVisitVarName.compareTo(varName) < 0 ? (hasVisitVarName + "-" + varName) : (varName + "-" + hasVisitVarName);
                    if (equalDPMap.containsKey(key)) {
                        hasDP = true;
                        sendDPMap.put(hasVisitVarName, equalDPMap.get(key));
                        previousOrNext.put(hasVisitVarName, query.compareSequence(hasVisitVarName, varName));
                    }
                }

                int headTailMarker = query.headTailMarker(varName);
                if(hasDP){
                    long startTime2 = System.currentTimeMillis();
                    List<RequestJoinInfoThread> requestJoinInfoThreads = new ArrayList<>(nodeNum);
                    for(BasicFilterBasedRPC.Client client : clients){
                        RequestJoinInfoThread thread = new RequestJoinInfoThread(client, varName, window, sendDPMap, localIntervals.serialize());
                        thread.start();
                        requestJoinInfoThreads.add(thread);
                    }
                    for(RequestJoinInfoThread thread : requestJoinInfoThreads){
                        try{ thread.join(); }catch (Exception e){ e.printStackTrace(); }
                    }
                    Map<String, Map<Long, Set<String>>> aggregatedPairs = new HashMap<>();
                    for(RequestJoinInfoThread thread : requestJoinInfoThreads){
                        transmissionSize += thread.getCommunicationCost();

                        // Map<variable name, Map<window id, event column values>>
                        Map<String, Map<Long, Set<String>>> pairs = thread.getPairs();
                        for(Map.Entry<String, Map<Long, Set<String>>> entry : pairs.entrySet()) {
                            String key = entry.getKey();
                            Map<Long, Set<String>> value = entry.getValue();
                            if (!aggregatedPairs.containsKey(key)) {
                                aggregatedPairs.put(key, value);
                            } else {
                                Map<Long, Set<String>> existingMap = aggregatedPairs.get(key);
                                for (Map.Entry<Long, Set<String>> e : value.entrySet()) {
                                    Long timeKey = e.getKey();
                                    Set<String> eventSet = e.getValue();
                                    if (!existingMap.containsKey(timeKey)) {
                                        existingMap.put(timeKey, eventSet);
                                    } else {
                                        existingMap.get(timeKey).addAll(eventSet);
                                    }
                                }
                            }
                        }
                    }

                    // join filtering
                    List<EqJoinFilterThread> eqJoinFilterThreads = new ArrayList<>(nodeNum);
                    for(BasicFilterBasedRPC.Client client : clients){
                        EqJoinFilterThread thread = new EqJoinFilterThread(client, varName, window, headTailMarker, previousOrNext, sendDPMap, aggregatedPairs);
                        thread.start();
                        eqJoinFilterThreads.add(thread);
                    }
                    for(EqJoinFilterThread thread : eqJoinFilterThreads){
                        try{ thread.join(); }catch (Exception e){ e.printStackTrace(); }
                    }
                    ReplayIntervals receivedIntervals = null;
                    for(EqJoinFilterThread thread : eqJoinFilterThreads){
                        transmissionSize += thread.getCommunicationCost();
                        ByteBuffer buffer = thread.getUpdatedIntervals();
                        ReplayIntervals ri = ReplayIntervals.deserialize(buffer);
                        if(receivedIntervals == null){
                            receivedIntervals = ri;
                        }else{
                            receivedIntervals.union(ri);
                        }
                    }
                    localIntervals.intersect(receivedIntervals);
                    long endTime2 = System.currentTimeMillis();
                    System.out.println("Join filtering on variable " + varName + " takes " + (endTime2 - startTime2) + " ms");
                }
                else{
                    // window filtering
                    List<WindowFilterThread> windowFilterThreads = new ArrayList<>();
                    for(BasicFilterBasedRPC.Client client : clients){
                        WindowFilterThread thread = new WindowFilterThread(client, varName, window, headTailMarker, localIntervals.serialize());
                        thread.start();
                        windowFilterThreads.add(thread);
                    }
                    for(WindowFilterThread thread : windowFilterThreads){
                        try{ thread.join(); }catch (Exception e){ e.printStackTrace(); }
                    }

                    ReplayIntervals receivedIntervals = null;
                    for(WindowFilterThread thread : windowFilterThreads){
                        transmissionSize += thread.getCommunicationCost();
                        ReplayIntervals ri = thread.getReplayIntervals();
                        if(receivedIntervals == null){
                            receivedIntervals = ri;
                        }else{
                            receivedIntervals.union(ri);
                        }
                    }
                    localIntervals.intersect(receivedIntervals);
                }
                hasProcessedVarName.add(varName);
            }
        }

        int recordLen = schema.getFixedRecordLen();
        List<PullEventThread> pullEventThreads = new ArrayList<>(nodeNum);
        for(BasicFilterBasedRPC.Client client : clients){
            PullEventThread thread = new PullEventThread(client, localIntervals.serialize(), recordLen);
            thread.start();
            pullEventThreads.add(thread);
        }
        List<byte[]> finalEvents = new ArrayList<>();
        for(PullEventThread thread : pullEventThreads){
            try{ thread.join(); }catch (Exception e){ e.printStackTrace(); }
            transmissionSize += thread.getCommunicationCost();
            finalEvents.addAll(thread.getEvents());
        }
        System.out.println("Total transmission size: " + transmissionSize + " bytes.");
        return finalEvents;
    }

    // here we only run synthetic dataset and use sase engine
    public static void runQueries(List<BasicFilterBasedRPC.Client> clients, String datasetName){
        EventSchema eventSchema = EventSchema.getEventSchema(datasetName);

        List<String> sqlList = ReadQueries.getQueryList(datasetName, false);
        for(int i = 0; i < sqlList.size(); ++i){
            System.out.println("query id: " + i);
            long queryStart = System.currentTimeMillis();
            String sql = sqlList.get(i);
            long startTime = System.currentTimeMillis();
            List<byte[]> byteRecords = communicate(sql, clients);
            long endTime = System.currentTimeMillis();
            System.out.println("final event size: " + byteRecords.size());
            byteRecords = SortByTs.sort(byteRecords, eventSchema);
            System.out.println("pull event time: " + (endTime - startTime) + "ms");
            EvaluationEngineSase.processQuery(byteRecords, sql);
            long queryEnd = System.currentTimeMillis();
            System.out.println("this query cost: " + (queryEnd - queryStart) + "ms");
        }
    }

    public static void main(String[] args) throws Exception {
        TConfiguration conf = new TConfiguration(Args.maxMassageLen, Args.maxMassageLen, Args.recursionLimit);

//        String sep = File.separator;
//        String filePath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep + "output" + sep + "naivemulti.txt";
//        System.setOut(new PrintStream(filePath));

        // please modify two lines
        String[] storageNodeIps = {"localhost"};
        int[] ports = {9090, 9090};
        int nodeNum = storageNodeIps.length;
        int timeout = 0;
        List<TTransport> transports = new ArrayList<>(nodeNum);

        List<BasicFilterBasedRPC.Client> clients = new ArrayList<>(nodeNum);
        for(int i = 0; i < nodeNum; ++i){
            TSocket socket = new TSocket(conf, storageNodeIps[i], ports[i], timeout);
            TTransport transport = new TFramedTransport(socket, Args.maxMassageLen);
            TProtocol protocol = new TBinaryProtocol(transport);
            BasicFilterBasedRPC.Client client = new BasicFilterBasedRPC.Client(protocol);
            transport.open();
            clients.add(client);
            transports.add(transport);
        }

        String datasetName = "SYNTHETIC";
        LocalDateTime now = LocalDateTime.now();
        System.out.println("Start time " + now);
        long start = System.currentTimeMillis();
        // please modify datasetName, isEsper to change running mode
        runQueries(clients, datasetName);
        long end = System.currentTimeMillis();
        now = LocalDateTime.now();
        System.out.println("Finish time " + now);
        System.out.println("Take " + (end - start) + " ms...");
        for(TTransport transport : transports){
            transport.close();
        }
    }
}
