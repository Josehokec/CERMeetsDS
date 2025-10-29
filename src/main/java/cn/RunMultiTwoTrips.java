package cn;

import compute.Args;
import compute.EvaluationEngineSase;
import rpc2.iface.DataChunk;
import rpc2.iface.TwoTripsRPC2;
import rpc2.iface.TwoTripsDataChunk;
import org.apache.thrift.TException;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.thrift.protocol.TBinaryProtocol;
import parser.QueryParse;
import request.ReadQueries;
import store.EventSchema;
import utils.ReplayIntervals;
import rpc2.iface.TsAttrPair;
import java.time.LocalDateTime;
import java.util.*;
import parser.EqualDependentPredicate;
import java.nio.ByteBuffer;
import utils.SortByTs;

public class RunMultiTwoTrips {

    static class MultiUser extends Thread{
        private final String clientId;
        List<TTransport> transports;
        List<TwoTripsRPC2.Client> clients;
        private final String datasetName;
        private final List<String> sqlList;

        public MultiUser(String clientId, String datasetName, int userNum, int userId){
            this.clientId = clientId;
            this.datasetName = datasetName;

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
                    TwoTripsRPC2.Client client = new TwoTripsRPC2.Client(protocol);
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

    static class FirstTripThread extends Thread{
        private final TwoTripsRPC2.Client client;
        private final String clientId;
        private final String tableName;
        private final String sql;

        private Map<String, ReplayIntervals> replayIntervalMap;
        private Map<String, Set<TsAttrPair>> tsAttrPairMap;
        private int communicationCost;

        public FirstTripThread(TwoTripsRPC2.Client client, String clientId, String tableName, String sql){
            this.client = client;
            this.clientId = clientId;
            this.tableName = tableName;
            this.sql = sql;

            replayIntervalMap = new HashMap<>(8);
            tsAttrPairMap = new HashMap<>(8);
        }

        public int getCommunicationCost() {
            return communicationCost;
        }

        public Map<String, ReplayIntervals> getReplayIntervalMap(){
            return replayIntervalMap;
        }

        public Map<String, Set<TsAttrPair>> getTsAttrPairMap(){
            return tsAttrPairMap;
        }

        @Override
        public void run(){
            try{
                boolean isLastChunk;
                int offset = 0;
                do{
                    TwoTripsDataChunk chunk = client.pullBasicInfo(clientId, tableName, sql, offset);
                    if(chunk.intervalMap != null){
                        for(String varName : chunk.intervalMap.keySet()){
                            ByteBuffer buffer = chunk.intervalMap.get(varName);
                            ReplayIntervals replayIntervals = ReplayIntervals.deserialize(buffer);
                            replayIntervalMap.put(varName, replayIntervals);
                            communicationCost += buffer.capacity() + varName.length();
                        }
                    }

                    int maxLen = 0;
                    if(!chunk.pairMap.isEmpty()){
                        for(String varName : chunk.pairMap.keySet()){
                            Set<TsAttrPair> pairs = chunk.pairMap.get(varName);
                            if(tsAttrPairMap.get(varName) == null){
                                tsAttrPairMap.put(varName, pairs);
                            }else{
                                tsAttrPairMap.get(varName).addAll(pairs);
                            }
                            maxLen = Math.max(maxLen, pairs.size());

                            for(TsAttrPair item : pairs){
                                communicationCost += (8 + item.attrValue.length());
                            }
                        }
                    }
                    offset += maxLen;
                    isLastChunk = chunk.isLastChunk;
                    communicationCost += tableName.length() + sql.length() + 4;
                }while(!isLastChunk);

            }catch (TException e){
                e.printStackTrace();
            }
        }
    }

    static class SecondTripThread extends Thread{
        private final TwoTripsRPC2.Client client;
        private final ByteBuffer replayIntervalsBuffer;
        private final String clientId;
        private final int recordSize;
        private List<byte[]> events;
        private long communicationCost;

        public SecondTripThread(TwoTripsRPC2.Client client, String clientId, ByteBuffer replayIntervalsBuffer, int recordSize){
            this.client = client;
            this.clientId = clientId;
            this.replayIntervalsBuffer = replayIntervalsBuffer;
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
                    DataChunk chunk = client.pullEvents(clientId, offset, replayIntervalsBuffer);
                    communicationCost += 4 + replayIntervalsBuffer.capacity();
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

    public static List<byte[]> communicate(String clientId, String sql, List<TwoTripsRPC2.Client> clients) {
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        long transmissionSize = 0;

        int nodeNum = clients.size();
        EventSchema schema = EventSchema.getEventSchema(tableName);
        int recordSize = schema.getFixedRecordLen();

        List<FirstTripThread> firstTripThreads = new ArrayList<>(nodeNum);
        for(TwoTripsRPC2.Client client : clients){
            FirstTripThread thread = new FirstTripThread(client, clientId, tableName, sql);
            thread.start();
            firstTripThreads.add(thread);
        }
        for(FirstTripThread t : firstTripThreads){
            try{t.join();}catch (Exception e){e.printStackTrace();}
        }

        //long time1Start = System.currentTimeMillis();
        // union replay intervals
        Map<String, ReplayIntervals> replayIntervalsMap = null;
        for(FirstTripThread t : firstTripThreads){
            transmissionSize += t.getCommunicationCost();
            if(replayIntervalsMap == null){
                replayIntervalsMap = t.getReplayIntervalMap();
            }else{
                // union
                Map<String, ReplayIntervals> unionMap = t.getReplayIntervalMap();
                for(String varName : unionMap.keySet()){
                    ReplayIntervals curIntervals = unionMap.get(varName);
                    if(replayIntervalsMap.get(varName) == null){
                        replayIntervalsMap.put(varName, curIntervals);
                    }else{
                        replayIntervalsMap.get(varName).union(curIntervals);
                    }
                }
            }
        }

        Map<String, Long> varIntervalLenMap = new HashMap<>(replayIntervalsMap.size() << 1);

        ReplayIntervals finalIntervals = null;
        for(Map.Entry<String, ReplayIntervals> entry : replayIntervalsMap.entrySet()){
            String varName = entry.getKey();
            ReplayIntervals curIntervals = entry.getValue();
            varIntervalLenMap.put(varName, curIntervals.getTimeLength());

            if(finalIntervals == null){
                finalIntervals = curIntervals;
            }else{
                finalIntervals.intersect(curIntervals);
            }
        }

        // union <timestamp, attribute value> pairs
        Map<String, Set<TsAttrPair>> tsAttrPairMap = null;
        for(FirstTripThread t : firstTripThreads){
            if(tsAttrPairMap == null){
                tsAttrPairMap = t.getTsAttrPairMap();
            }else{
                // union
                Map<String, Set<TsAttrPair>> unionMap = t.getTsAttrPairMap();
                for(String varName : unionMap.keySet()){
                    Set<TsAttrPair> pairs = unionMap.get(varName);
                    if(tsAttrPairMap.get(varName) == null){
                        tsAttrPairMap.put(varName, pairs);
                    }else{
                        tsAttrPairMap.get(varName).addAll(pairs);
                    }
                }
            }
        }

        long window = query.getWindow();
        Map<String, Map<Long, Set<String>>> windowAttrMap = new HashMap<>(tsAttrPairMap.size() << 1);
        for(Map.Entry<String, Set<TsAttrPair>> entry : tsAttrPairMap.entrySet()){
            String varAttrName = entry.getKey();
            Set<TsAttrPair> pairs = entry.getValue();
            // check timestamp...
            for(TsAttrPair pair : pairs){
                long ts = pair.timestamp;
                if(finalIntervals.contains(ts)) {
                    windowAttrMap.computeIfAbsent(varAttrName, k -> new HashMap<>()).computeIfAbsent(ts / window, k -> new HashSet<>()).add(pair.attrValue);
                }
            }
        }

        // we only consider A.x = B.y, do not consider A.x = B.y + 1
        Map<String, List<String>> equalDpStrMap = query.getEqualDpMap();

        // define process order
        List<Map.Entry<String, Long>> entryList = new ArrayList<>(varIntervalLenMap.entrySet());
        entryList.sort(Map.Entry.comparingByValue());
        Set<String> hasProcessedVarNames = new HashSet<>(8);
        hasProcessedVarNames.add(entryList.get(0).getKey());

        for(int i = 1; i < entryList.size(); i++){
            String varName1 = entryList.get(i).getKey();

            long leftOffset, rightOffset;
            if(query.headTailMarker(varName1) == 0){
                // 0: head variable, 1: tail variable, 2: middle variable
                leftOffset = 0;
                rightOffset = window;
            }else if(query.headTailMarker(varName1) == 1){
                leftOffset = -window;
                rightOffset = 0;
            }else{
                leftOffset = -window;
                rightOffset = window;
            }

            for(String varName2 : hasProcessedVarNames){
                String key = varName1.compareTo(varName2) < 0 ? (varName1 + "-" + varName2) : (varName2 + "-" + varName1);
                if(equalDpStrMap.containsKey(key)){
                    // false: previous, true: next
                    boolean isNext = query.compareSequence(varName2, varName1);
                    List<String> equalDpList = equalDpStrMap.get(key);

                    for(String equalDp : equalDpList){
                        ReplayIntervals newIntervals = new ReplayIntervals();
                        EqualDependentPredicate edp = new EqualDependentPredicate(equalDp);
                        String key1 = varName1 + "-" + edp.getAttrName(varName1);
                        String key2 = varName2 + "-" + edp.getAttrName(varName2);

                        int fixOffset = isNext ? 1 : -1;
                        Set<TsAttrPair> pairs = tsAttrPairMap.get(key1);
                        for(TsAttrPair pair : pairs){
                            long ts = pair.timestamp;
                            Map<Long, Set<String>> joinMap = windowAttrMap.get(key2);
                            if(finalIntervals.contains(ts)){
                                long wid = ts/window;
                                if((joinMap.containsKey(wid) && joinMap.get(wid).contains(pair.attrValue)) ||
                                        (joinMap.containsKey(wid + fixOffset) && joinMap.get(wid + fixOffset).contains(pair.attrValue))){
                                    newIntervals.insert(ts + leftOffset, ts + rightOffset);
                                }
                            }
                        }

                        finalIntervals.intersect(newIntervals);

                        // debug
                        // System.out.println("equal predicate -> interval len: " + finalIntervals.getTimeLength());
                    }
                }
            }
            hasProcessedVarNames.add(varName1);
        }

        ByteBuffer buffer = finalIntervals.serialize();
        List<SecondTripThread> pullThreads = new ArrayList<>(nodeNum);
        for(TwoTripsRPC2.Client client : clients){
            SecondTripThread t = new SecondTripThread(client, clientId, buffer, recordSize);
            t.start();
            pullThreads.add(t);
        }

        for(SecondTripThread t : pullThreads){
            try{
                t.join();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        List<byte[]> filteredEvent = null;
        for(SecondTripThread t : pullThreads){
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

    public static void runQueries(String clientId, List<String> sqlList, List<TwoTripsRPC2.Client> clients){
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
