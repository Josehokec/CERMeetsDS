package compute;

import filter.OptimizedSWF;
import filter.UpdatedMarkers;
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
import utils.ReplayIntervals;
import utils.SortByTs;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;

public class RunSwfHashTable {

    static class InitialThread extends Thread {
        private final BasicFilterBasedRPC.Client client;
        private final String tableName;
        // ip: independent predicate
        private final Map<String, List<String>> ipMap;
        private Map<String, Integer> varEventNumMap;
        private long communicationCost;

        public InitialThread(BasicFilterBasedRPC.Client client, String tableName, Map<String, List<String>> ipMap) {
            this.client = client;
            this.tableName = tableName;
            this.ipMap = ipMap;
            this.communicationCost = 0L;
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public Map<String, Integer> getVarEventNumMap() {
            return varEventNumMap;
        }

        @Override
        public void run() {
            try{
                varEventNumMap = client.initial(tableName, ipMap);
                communicationCost += tableName.length() + ipMap.toString().length();
                communicationCost += varEventNumMap.toString().length();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class GenerateIntervalsThread extends Thread {
        private final BasicFilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private ReplayIntervals ri;
        private long communicationCost;

        public GenerateIntervalsThread(BasicFilterBasedRPC.Client client, String varName, long window, int headTailMarker) {
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.communicationCost = 0L;
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public ReplayIntervals getReplayIntervals() {
            return ri;
        }

        @Override
        public void run() {
            try{
                ByteBuffer intervalBuffer = client.getInitialIntervals(varName, window, headTailMarker);
                communicationCost += varName.length() + 8 + 4; // long + int
                communicationCost += intervalBuffer.capacity();
                ri = ReplayIntervals.deserialize(intervalBuffer);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class WindowFilterThread extends Thread {
        private final BasicFilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final ByteBuffer swfBuffer;

        private UpdatedMarkers updateMarkers;
        private int filteredNum;
        private long communicationCost;

        public WindowFilterThread(BasicFilterBasedRPC.Client client, String varName, long window, int headTailMarker, ByteBuffer swfBuffer) {
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.swfBuffer = swfBuffer;
            this.communicationCost = 0L;
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public UpdatedMarkers getUpdateMarkers(){
            return updateMarkers;
        }

        public int getFilteredNum() {
            return filteredNum;
        }

        @Override
        public void run() {
            try{
                ByteBuffer markers = client.windowFilter(varName, window, headTailMarker, swfBuffer);
                communicationCost += varName.length() + 8 + 4; // long + int
                communicationCost += markers.capacity();
                updateMarkers = UpdatedMarkers.deserialize(markers);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class RequestJoinInfoThread extends Thread {
        private final BasicFilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final Map<String, List<String>> dpMap;
        private final ByteBuffer swfBuffer;

        private Map<String, Map<Long, java.util.Set<String>>> pairs;
        private long communicationCost;

        public RequestJoinInfoThread(BasicFilterBasedRPC.Client client, String varName, long window, Map<String, List<String>> dpMap, ByteBuffer swfBuffer) {
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.dpMap = dpMap;
            this.swfBuffer = swfBuffer;
            this.communicationCost = 0L;
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public Map<String, Map<Long, java.util.Set<String>>> getPairs() {
            return pairs;
        }

        @Override
        public void run() {
            try{
                pairs = client.getHashTable4EQJoin(varName, window, dpMap, swfBuffer);
                communicationCost += varName.length() + 8; // long
                communicationCost += dpMap.toString().length();
                communicationCost += pairs.toString().length();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class EqJoinFilterThread extends Thread {
        private final BasicFilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final Map<String, Boolean> previousOrNext;
        private final Map<String, List<String>> dpMap;
        private final Map<String, Map<Long, java.util.Set<String>>> pairs;

        private UpdatedMarkers updatedMarkers;
        private long communicationCost;

        public EqJoinFilterThread(BasicFilterBasedRPC.Client client, String varName, long window, int headTailMarker, Map<String, Boolean> previousOrNext, Map<String, List<String>> dpMap, Map<String, Map<Long, java.util.Set<String>>> pairs) {
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.previousOrNext = previousOrNext;
            this.dpMap = dpMap;
            this.pairs = pairs;
            this.communicationCost = 0L;
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public UpdatedMarkers getUpdatedMarkers() {
            return updatedMarkers;
        }

        @Override
        public void run() {
            try{
                ByteBuffer ans = client.eqJoinFilter(varName, window, headTailMarker, previousOrNext, dpMap, pairs);
                communicationCost += varName.length() + 8 + 4; // long + int
                communicationCost += previousOrNext.toString().length();
                communicationCost += dpMap.toString().length();
                communicationCost += pairs.toString().length();
                communicationCost += ans.capacity();
                updatedMarkers = UpdatedMarkers.deserialize(ans);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class PullEventThread extends Thread {
        private final BasicFilterBasedRPC.Client client;
        private final long window;
        private final ByteBuffer swfBuffer;
        private final int recordLen;

        private long communicationCost;
        private List<byte[]> events;

        public PullEventThread(BasicFilterBasedRPC.Client client, long window, ByteBuffer swfBuffer, int recordLen) {
            this.client = client;
            this.window = window;
            this.swfBuffer = swfBuffer;
            this.recordLen = recordLen;
            this.communicationCost = 0L;
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
                do{
                    SDataChunk chunk;
                    if(isLastChunk){
                        chunk  = client.getAllFilteredEvents(window, swfBuffer);
                    }else{
                        chunk  = client.getAllFilteredEvents(window, null);
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
        List<InitialThread> initialThreads = new ArrayList<>(nodeNum);
        for(BasicFilterBasedRPC.Client client : clients){
            InitialThread thread = new InitialThread(client, tableName, ipMap);
            initialThreads.add(thread);
            thread.start();
        }
        for(InitialThread thread : initialThreads){
            try{
                thread.join();
            }catch (Exception e){
                e.printStackTrace();
            }
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

        OptimizedSWF optimizedSWF = null;
        UpdatedMarkers updatedMarkers = null;

        Plan plan  = GeneratedPlan.filterPlan(varEventNumMap, query.getHeadVarName(), query.getTailVarName(), query.getDpMap());
        List<String> steps = plan.getSteps();
        Set<String> hasProcessedVarName = new HashSet<>();

        for(String varName : steps) {
            if (hasProcessedVarName.isEmpty()) {
                List<GenerateIntervalsThread> genIntervalThread = new ArrayList<>(nodeNum);
                for(BasicFilterBasedRPC.Client client : clients){
                    GenerateIntervalsThread thread = new GenerateIntervalsThread(client, varName, window, query.headTailMarker(varName));
                    genIntervalThread.add(thread);
                    thread.start();
                }
                for(GenerateIntervalsThread thread : genIntervalThread){
                    try{thread.join();}catch (Exception e){e.printStackTrace();}
                }

                ReplayIntervals replayIntervals = null;
                for(GenerateIntervalsThread t : genIntervalThread){
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
                    List<RequestJoinInfoThread> requestJoinInfoThreads = new ArrayList<>(nodeNum);
                    for(BasicFilterBasedRPC.Client client : clients){
                        RequestJoinInfoThread thread = new RequestJoinInfoThread(client, varName, window, sendDPMap, optimizedSWF.serialize());
                        requestJoinInfoThreads.add(thread);
                        thread.start();
                    }
                    for(RequestJoinInfoThread thread : requestJoinInfoThreads){
                        try{thread.join();}catch (Exception e){e.printStackTrace();}
                    }

                    Map<String, Map<Long, Set<String>>> mergedPairs = new HashMap<>();
                    for(RequestJoinInfoThread t : requestJoinInfoThreads){
                        transmissionSize += t.getCommunicationCost();
                        Map<String, Map<Long, Set<String>>> pairs = t.getPairs();
                        for(String preVarName : pairs.keySet()){
                            Map<Long, Set<String>> preVarPairs = pairs.get(preVarName);
                            if(!mergedPairs.containsKey(preVarName)){
                                mergedPairs.put(preVarName, preVarPairs);
                            }else{
                                Map<Long, Set<String>> existPairs = mergedPairs.get(preVarName);
                                for(Long key : preVarPairs.keySet()){
                                    Set<String> values = preVarPairs.get(key);
                                    if(!existPairs.containsKey(key)){
                                        existPairs.put(key, values);
                                    }else{
                                        existPairs.get(key).addAll(values);
                                    }
                                }
                            }
                        }
                    }

                    List<EqJoinFilterThread> eqJoinFilterThreads = new ArrayList<>(nodeNum);
                    for(BasicFilterBasedRPC.Client client : clients){
                        EqJoinFilterThread thread = new EqJoinFilterThread(client, varName, window, headTailMarker, previousOrNext, sendDPMap, mergedPairs);
                        eqJoinFilterThreads.add(thread);
                        thread.start();
                    }
                    for(EqJoinFilterThread thread : eqJoinFilterThreads){
                        try{thread.join();}catch (Exception e){e.printStackTrace();}
                    }

                    updatedMarkers = null;
                    for(EqJoinFilterThread t : eqJoinFilterThreads) {
                        transmissionSize += t.getCommunicationCost();
                        if (updatedMarkers == null) {
                            updatedMarkers = t.getUpdatedMarkers();
                        } else {
                            updatedMarkers.merge(t.getUpdatedMarkers());
                        }
                    }

                    optimizedSWF.rebuild(updatedMarkers);
                }
                else{
                    List<WindowFilterThread> windowFilterThreads = new ArrayList<>(nodeNum);
                    for(BasicFilterBasedRPC.Client client : clients){
                        WindowFilterThread thread = new WindowFilterThread(client, varName, window, headTailMarker, optimizedSWF.serialize());
                        windowFilterThreads.add(thread);
                        thread.start();
                    }
                    for(WindowFilterThread thread : windowFilterThreads){
                        try{thread.join();}catch (Exception e){e.printStackTrace();}
                    }

                    updatedMarkers = null;
                    for(WindowFilterThread t : windowFilterThreads) {
                        transmissionSize += t.getCommunicationCost();
                        if (updatedMarkers == null) {
                            updatedMarkers = t.getUpdateMarkers();
                        } else {
                            updatedMarkers.merge(t.getUpdateMarkers());
                        }
                    }
                }
                optimizedSWF.rebuild(updatedMarkers);
                hasProcessedVarName.add(varName);
            }
        }

        int recordLen = schema.getFixedRecordLen();
        ByteBuffer swfBuffer = optimizedSWF.serialize();
        List<PullEventThread> pullEventThreads = new ArrayList<>(nodeNum);
        for(BasicFilterBasedRPC.Client client : clients){
            PullEventThread thread = new PullEventThread(client, window, swfBuffer, recordLen);
            pullEventThreads.add(thread);
            thread.start();
        }
        for(PullEventThread thread : pullEventThreads){
            try{thread.join();}catch (Exception e){e.printStackTrace();}
        }
        List<byte[]> byteRecords = new ArrayList<>();
        for(PullEventThread t : pullEventThreads){
            transmissionSize += t.getCommunicationCost();
            List<byte[]> events = t.getAllEvents();
            byteRecords.addAll(events);
        }
        System.out.println("transmission cost: " + transmissionSize + " bytes.");
        return byteRecords;
    }

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


    public static void main(String[] args) throws Exception{
        TConfiguration conf = new TConfiguration(Args.maxMassageLen, Args.maxMassageLen, Args.recursionLimit);

//        String sep = File.separator;
//        String filePath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep + "output" + sep + "swfhashtable.txt";
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
