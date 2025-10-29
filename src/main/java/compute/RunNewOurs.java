package compute;

import filter.OptimizedSWF;
import filter.SWF;
import filter.UpdatedMarkers;
import filter.WindowWiseBF;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import parser.EqualDependentPredicate;
import parser.QueryParse;
import plan.*;
import request.ReadQueries;
import rpc.iface.FilterBasedRPC;
import rpc.iface.FilteredResult;
import rpc.iface.SameDataChunk;
import store.EventSchema;
import utils.Pair;
import utils.ReplayIntervals;
import utils.SortByTs;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;

public class RunNewOurs {

    static double networkBandwidth = 0.1; // Byte/ns

    static class InitialThread extends Thread {
        private final FilterBasedRPC.Client client;
        private final String tableName;
        // ip: independent predicate
        private final Map<String, List<String>> ipMap;
        private Map<String, Integer> varEventNumMap;
        private long communicationCost;

        public InitialThread(FilterBasedRPC.Client client, String tableName, Map<String, List<String>> ipMap){
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
        private final FilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private ReplayIntervals ri;
        private long communicationCost;

        public GenerateIntervalThread(FilterBasedRPC.Client client, String varName, long window, int headTailMarker){
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
        private final FilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final ByteBuffer swfBuffer;

        private UpdatedMarkers updateMarkers;
        private int filteredNum;
        private long communicationCost;

        public WindowFilterThread(FilterBasedRPC.Client client, String varName, long window, int headTailMarker, ByteBuffer swfBuffer){
            this.client = client;
            this.varName = varName;
            this.window = window;
            this.headTailMarker = headTailMarker;
            this.swfBuffer = swfBuffer;
            communicationCost = 0;
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
                FilteredResult res  = client.windowFilter(varName, window, headTailMarker, swfBuffer);
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
        private final FilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final Map<String, List<String>> sendDPMap;
        private final Map<String, Integer> eventNumMap;
        private final ByteBuffer swf;
        private Map<String, ByteBuffer> bfBufferMap;
        private long communicationCost;

        public Map<String, ByteBuffer> getBfBufferMap(){
            return bfBufferMap;
        }

        public long getCommunicationCost() {
            return communicationCost;
        }

        public RequestBloomFilterThread(FilterBasedRPC.Client client, String varName, long window,
                                        Map<String, List<String>> sendDPMap, Map<String, Integer> eventNumMap, ByteBuffer swf){
            this.client = client;
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
                bfBufferMap = client.getBF4EQJoin(varName, window, sendDPMap, eventNumMap, swf);
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
        private final FilterBasedRPC.Client client;
        private final String varName;
        private final long window;
        private final int headTailMarker;
        private final Map<String, Boolean> previousOrNext;
        private final Map<String, List<String>> sendDPMap;
        private final Map<String, ByteBuffer> bfBufferMap;
        private UpdatedMarkers updatedMarkers;
        private SWF updatedSWF;
        private long communicationCost;

        private int filteredNum;

        public EqJoinFilterThread(FilterBasedRPC.Client client, String varName, long window, int headTailMarker,
                                  Map<String, Boolean> previousOrNext, Map<String, List<String>> sendDPMap, Map<String, ByteBuffer> bfBufferMap){
            this.client = client;
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

        public SWF getUpdatedSWF(){
            return updatedSWF;
        }

        public int getFilteredNum() {
            return filteredNum;
        }

        @Override
        public void run() {
            try{
                FilteredResult ans  = client.eqJoinFilter(varName, window, headTailMarker, previousOrNext, sendDPMap, bfBufferMap);
                communicationCost += ans.updatedSWF.capacity() + 4;
                filteredNum = ans.filteredNum;
                if(Args.isOptimizedSwf){
                    updatedMarkers = UpdatedMarkers.deserialize(ans.updatedSWF);
                }else{
                    updatedSWF = SWF.deserialize(ans.updatedSWF);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class PullEventThread extends Thread{
        private final FilterBasedRPC.Client client;
        private final long window;
        private final ByteBuffer swfBuffer;
        private final int recordLen;

        private List<byte[]> events;
        private long communicationCost;

        public PullEventThread(FilterBasedRPC.Client client, long window, ByteBuffer swfBuffer, int recordLen){
            this.client = client;
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
                    SameDataChunk chunk;
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

    public static List<byte[]> communicate(String sql, List<FilterBasedRPC.Client> clients, CostModelType type){
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
        for(FilterBasedRPC.Client client : clients){
            InitialThread initialThread = new InitialThread(client, tableName, ipMap);
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
        CostEstimator costEstimator;

        long modelStart = System.currentTimeMillis();
        switch (type){
            case NAIVE:
                costEstimator = new NaiveCostModel(nodeNum, query, varEventNumMap, schema);
                break;
            case ALWAYS_ON:
                costEstimator = new AlwaysOnCostModel(nodeNum, query, varEventNumMap, schema);
                break;
            case ALWAYS_OFF:
                costEstimator = new AlwaysOffCostModel(nodeNum, query, varEventNumMap, schema);
                break;
            case OLD_MODEL:
                costEstimator = new ConstantCostModel(nodeNum, query, varEventNumMap, schema);
                break;
            default:
                costEstimator = new AdaptiveCostModel(nodeNum, query, varEventNumMap, schema);
        }
        long modelEnd = System.currentTimeMillis();
        System.out.println("model cost: " + (modelEnd - modelStart) + "ms");
        for(String varName : steps){
            System.out.println("processing variable: " + varName);
            if(hasProcessedVarName.isEmpty()){
                // initialization...
                List<GenerateIntervalThread> generateIntervalThreads = new ArrayList<>(nodeNum);
                for(FilterBasedRPC.Client client : clients){
                    GenerateIntervalThread thread = new GenerateIntervalThread(client, varName, window, query.headTailMarker(varName));
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

                // used for cost model
                int bfSize = 0;
                double keyNum = 0;
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
                        keyNum += estimateEventNum;
                        bfSize += WindowWiseBF.getEstimatedBitSize(estimateEventNum);
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

                long bitCount = optimizedSWF.getSliceNum();

                System.out.println("sendDPMap.size: " + sendDPMap.size());
                boolean startRoundTrip = costEstimator.newRoundTrip(bitCount, query.headTailMarker(varName),
                        varName, swfBuffer.capacity(), bfSize, sendDPMap.size(), 8, (int) keyNum, networkBandwidth);

                if(startRoundTrip){
                    long startTime1 = System.nanoTime();
                    if(hasDP){
                        // long startTime2 = System.currentTimeMillis();
                        List<RequestBloomFilterThread> requestBloomFilterThreads = new ArrayList<>(nodeNum);
                        for (FilterBasedRPC.Client client : clients) {
                            RequestBloomFilterThread thread = new RequestBloomFilterThread(client, varName, window, sendDPMap, eventNumMap, swfBuffer);
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
                                    byte[] mergedBF = RunOurs.orByteArrays(byteArray, mergedBFBufferMap.get(key));
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
                        for (FilterBasedRPC.Client client : clients) {
                            EqJoinFilterThread thread = new EqJoinFilterThread(client, varName, window, headTailMarker, previousOrNext, sendDPMap, bfBufferMap);
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

                        // long endTime2 = System.currentTimeMillis();
                        // System.out.println("current varName: " + varName + " build bf cost: " + (endTime2 - startTime2) + "ms");
                    }
                    else{
                        // only use window condition
                        List<WindowFilterThread> windowFilterThreads = new ArrayList<>(nodeNum);

                        for (FilterBasedRPC.Client client : clients) {
                            WindowFilterThread thread = new WindowFilterThread(client, varName, window, query.headTailMarker(varName), swfBuffer);
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
                    long endTime1 = System.nanoTime();
                    System.out.println("new round trip real cost time: " + (endTime1 - startTime1) + "ns");

                    optimizedSWF.rebuild(updatedMarkers);
                    slicedBits = optimizedSWF.getSliceNum();
                }
                else{
                    updatedEventNum = varEventNumMap.get(varName);
                }
                filteredEventNum.put(varName, new Pair<>(updatedEventNum, slicedBits));
                hasProcessedVarName.add(varName);
            }
        }

        int recordLen = schema.getFixedRecordLen();
        ByteBuffer swfBuffer = optimizedSWF.serialize();

        long s = System.nanoTime();
        List<PullEventThread> pullEventThreads = new ArrayList<>(nodeNum);
        for (FilterBasedRPC.Client client : clients) {
            PullEventThread thread = new PullEventThread(client, window, swfBuffer, recordLen);
            thread.start();
            pullEventThreads.add(thread);
        }
        for (PullEventThread thread : pullEventThreads){
            try{ thread.join(); } catch (Exception e){ e.printStackTrace(); }
        }
        long e = System.nanoTime();

        List<byte[]> byteRecords = null;
        for (PullEventThread thread : pullEventThreads){
            transmissionSize += thread.getCommunicationCost();
            if(byteRecords == null){
                byteRecords = thread.getAllEvents();
            }else{
                byteRecords.addAll(thread.getAllEvents());
            }
        }
        networkBandwidth = transmissionSize * 1.0 / (e - s);
        System.out.println("estimated network bandwidth: " + networkBandwidth + " Byte/ns");
        System.out.println("transmission cost: " + transmissionSize + " bytes");
        return byteRecords;
    }

    public static void runQueries(List<FilterBasedRPC.Client> clients, String datasetName){
        EventSchema eventSchema = EventSchema.getEventSchema(datasetName);
        // String sqlPah = Paths.get("src/main/java/request/synthetic_query_cost_model.txt").toAbsolutePath().toString();
        // List<String> sqlList = ReadQueries.readQueries(sqlPah);
        List<String> sqlList = ReadQueries.getQueryList(datasetName, false);

        List<CostModelType> modelTypes = new ArrayList<>(Arrays.asList(
                CostModelType.NAIVE,
                CostModelType.ALWAYS_ON,
                CostModelType.ALWAYS_OFF,
                CostModelType.OLD_MODEL,
                CostModelType.ADA_MODEL
        ));

        // sqlList.size()， please note that 170 is correct!!!!
        for(int i = 0; i < 170; i++) {
            for(CostModelType type : modelTypes){
                System.out.println("query id: " + i);
                long queryStart = System.currentTimeMillis();
                String sql = sqlList.get(i);
                // System.out.println(sql);
                long startTime = System.currentTimeMillis();
                List<byte[]> byteRecords = communicate(sql, clients, type);
                long endTime = System.currentTimeMillis();
                System.out.println("final event size: " + byteRecords.size());
                byteRecords = SortByTs.sort(byteRecords, eventSchema);

                System.out.println("pull event time: " + (endTime - startTime) + "ms");

                // EvaluationEngineSase.processQuery(byteRecords, sql, SelectionStrategy.STRICT_CONTIGUOUS);
                EvaluationEngineSase.processQuery(byteRecords, sql);
                long queryEnd = System.currentTimeMillis();
                System.out.println("this query cost: " + (queryEnd - queryStart) + "ms");
            }
        }
    }

    enum CostModelType{NAIVE, ALWAYS_ON, ALWAYS_OFF, OLD_MODEL, ADA_MODEL}

    public static void main(String[] args) throws Exception {
        TConfiguration conf = new TConfiguration(Args.maxMassageLen, Args.maxMassageLen, Args.recursionLimit);

//        String sep = File.separator;
//        String filePath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep + "output" + sep + "pushpull_cluster_sase.txt";
//        System.setOut(new PrintStream(filePath));

        // please modify two lines
        String[] storageNodeIps = {"localhost"};
        int[] ports = {9090, 9090};
        int nodeNum = storageNodeIps.length;
        int timeout = 0;
        List<TTransport> transports = new ArrayList<>(nodeNum);

        List<FilterBasedRPC.Client> clients = new ArrayList<>(nodeNum);
        for(int i = 0; i < nodeNum; ++i) {
            TSocket socket = new TSocket(conf, storageNodeIps[i], ports[i], timeout);
            TTransport transport = new TFramedTransport(socket, Args.maxMassageLen);
            TProtocol protocol = new TBinaryProtocol(transport);
            FilterBasedRPC.Client client = new FilterBasedRPC.Client(protocol);
            // when we open, we can call related interface
            transport.open();
            clients.add(client);
            transports.add(transport);
        }

        String datasetName = "SYNTHETIC";
        System.out.println("@args new ours, " + " #dataset: " + datasetName + " #isSaseEngine: " + Args.isSaseEngine);

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
