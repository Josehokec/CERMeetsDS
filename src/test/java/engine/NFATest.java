package engine;

import org.junit.Test;
import parser.QueryParse;
import store.EventSchema;
import utils.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


/*
4,1000,12345,28,1000
5,1200,12345,25,1500
6,1300,12345,85,2000
7,2000,123456,28,1000
8,2050,123456,25,1500
9,2100,123456,20,2000
10,2150,123456,83,1000
11,3100,1234,28,1000
12,3200,1234,25,1500
13,3250,1234,85,2000
14,4000,123,34,1000
15,4050,123,25,1500
16,4100,123,16,2000
17,4150,123,83,1000
 */
public class NFATest {

    @Test
    public void testMyNFA() {

        /*
        "SELECT * FROM stock MATCH_RECOGNIZE(" +
        "ORDER BY timestamp " +
        "MEASURES A.id as AID ONE ROW PER MATCH AFTER MATCH SKIP TO NEXT ROW " +
        "PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '1000' SECOND " +
        "DEFINE " +
        "    A AS A.symbol = 1 AND A.price < 30, " +
        "    B AS B.symbol = 7 AND B.price < A.price," +
        "    C AS C.symbol = 4 AND C.price > 80 \n" +
        ") MR;";
         */

        int MAX_LOOP = 1;
        for (int i = 0; i < MAX_LOOP; i++) {
            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String formattedNow = now.format(formatter);
            //System.out.println("start: " + formattedNow);

            /*
            result size: 402722
            Elapsed time: 5156 ms
             */
            String querySQL =
                    "SELECT * FROM stock MATCH_RECOGNIZE(" +
                    "partition by symbol " +
                    "ORDER BY timestamp " +
                    "MEASURES A.id as AID " +
                    "ONE ROW PER MATCH " +
                    "AFTER MATCH SKIP TO NEXT ROW " +
                    "PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '200' SECOND " +
                    "DEFINE " +
                    "    A AS A.price < 30, " +
                    "    B AS B.price < A.price," +
                    "    C AS C.price > 80 " +
                    ") MR;";
            QueryParse query = new QueryParse(querySQL);
            NFA nfa = new NFA();
            nfa.constructNFA(query);

            String fileName= "test_stock.csv";
            List<byte[]> events = obtainByteEvents(fileName);

            long startTime = System.nanoTime();
            EventSchema schema = EventSchema.getEventSchema("STOCK");
            int countEvents = 0;
            for (byte[] event : events) {
                nfa.consume(event, SelectionStrategy.STRICT_CONTIGUOUS, schema);
                countEvents++;
            }
            nfa.printAnyMatch(schema);
//            int count = nfa.getMatchesNum();
//            System.out.println("result size: " + count);


            long endTime = System.nanoTime();
            long elapsedTime = endTime - startTime;
            System.out.println("Elapsed time: " + elapsedTime / 1000000 + " ms");
            //nfa.display();

            now = LocalDateTime.now();
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            formattedNow = now.format(formatter);
            //System.out.println("start: " + formattedNow);
        }
    }

    public static List<byte[]> obtainByteEvents(String fileName){
        List<byte[]> byteEvents = new ArrayList<>(1024);
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length == 5) {
                    try {
                        int id = Integer.parseInt(values[0].trim());
                        long eventTime = Long.parseLong(values[1].trim());
                        int symbol = Integer.parseInt(values[2].trim());
                        int price = Integer.parseInt(values[3].trim());
                        int volume = Integer.parseInt(values[4].trim());

                        ByteBuffer byteEvent = ByteBuffer.allocate(24);
                        byteEvent.putInt(id);
                        byteEvent.putLong(eventTime);
                        byteEvent.putInt(symbol);
                        byteEvent.putInt(price);
                        byteEvent.putInt(volume);

                        byteEvents.add(byteEvent.array());
                    } catch (NumberFormatException e) {
                        System.err.println("Error parsing line: " + line);
                        e.printStackTrace();
                    }
                } else {
                    System.err.println("Invalid line format: " + line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byteEvents;
    }

    @Test
    public void simpleTest(){
        String querySQL =
                "SELECT * FROM Crimes MATCH_RECOGNIZE( ORDER BY timestamp MEASURES A.id as AID ONE ROW PER MATCH AFTER MATCH SKIP TO NEXT ROW " +
                        "PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '30' MINUTER " +
                        "DEFINE " +
                        "    A AS A.type = 'A' AND A.beat >= 1900 AND A.beat <= 2000, " +
                        "    B AS B.type = 'B' AND B.lot < A.lot," +
                        "    C AS C.type = 'C' AND C.district = B.district \n" +
                        ") MR;";
        QueryParse query = new QueryParse(querySQL);
        NFA nfa = new NFA();
        nfa.constructNFA(query);

        nfa.display();
    }

    @Test
    public void constructNFATest() {
        String querySQL =
                "SELECT * FROM Crimes MATCH_RECOGNIZE(\n" +
                        "    ORDER BY timestamp\n" +
                        "    MEASURES A.id as AID, B.id as BID, C.id AS CID\n" +
                        "    ONE ROW PER MATCH\n" +
                        "    AFTER MATCH SKIP TO NEXT ROW \n" +
                        "    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '30' MINUTER\n" +
                        "    DEFINE \n" +
                        "        A AS A.type = 'ROBBERY' AND A.beat >= 1900 AND A.beat <= 2000, \n" +
                        "        B AS B.type = 'BATTERY', \n" +
                        "        C AS C.type = 'MOTOR_VEHICLE_THEFT' AND C.district = B.district AND C.latitude + 0.0067 > B.latitude\n" + // AND C.district = B.district
                        ") MR;";


        QueryParse query = new QueryParse(querySQL);
        String tableName = query.getTableName();
        EventSchema schema = EventSchema.getEventSchema(tableName);

        NFA nfa = new NFA();
        nfa.constructNFA(query);
        nfa.display();

        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
        // crimes_q1_ultra: 1878
        // crimes_q1_ultra_join.txt
        // crimes_q1_12_10_10_old:
        // crimes_q1_interval
        String outputFilePath = prefixPath + "output" + sep + "crimes_mini.txt";

        List<Pair<Long, byte[]>> recordWithTsList = new ArrayList<>(1024);
        try (BufferedReader br = new BufferedReader(new FileReader(outputFilePath))) {
            // type,id,beat,district,latitude,longitude,timestamp
            String line;
            //line = br.readLine();
            while ((line = br.readLine()) != null) {
                byte[] record = schema.covertStringToBytes(line);
                recordWithTsList.add(new Pair<>(schema.getTimestamp(record), record));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        long readStart = System.currentTimeMillis();
        recordWithTsList.sort(Comparator.comparingLong(Pair::getKey));
        List<byte[]> sortedRecords = new ArrayList<>(recordWithTsList.size());
        for(Pair<Long, byte[]> recordWithTs : recordWithTsList){
            sortedRecords.add(recordWithTs.getValue());
        }
        long readEnd = System.currentTimeMillis();
        System.out.println("sort cost: " + (readEnd - readStart) + "ms");

        // please ensure it is ordered
//        FullScan fullScan = new FullScan(tableName);
//        List<byte[]> records = fullScan.scan(query.getIpStringMap());
//        System.out.println("records size: " + records.size());
//
//        long startSort = System.currentTimeMillis();
//        int size = records.size();
//        List<Pair<Long, byte[]>> recordWithTsList = new ArrayList<>(records.size());
//        for(byte[] record : records){
//            long ts = schema.getTimestamp(record);
//            recordWithTsList.add(new Pair<>(ts, record));
//        }
//        recordWithTsList.sort(Comparator.comparingLong(Pair::getKey));
//        List<byte[]> sortedRecords = new ArrayList<>(size);
//        for(Pair<Long, byte[]> recordWithTs : recordWithTsList){
//            sortedRecords.add(recordWithTs.getValue());
//        }
//        long endSort = System.currentTimeMillis();
//        System.out.println("sort cost: " + (endSort - startSort) + "ms");

        long start = System.currentTimeMillis();
        for(byte[] record : sortedRecords){
            nfa.consume(record, SelectionStrategy.SKIP_TILL_ANY_MATCH, schema);
        }
        long end = System.currentTimeMillis();
        // SKIP_TILL_ANY_MATCH
        // cost: 38ms
        // ans size: 22909
        System.out.println("match cost: " + (end - start) + "ms");
        // nfa.printAnyMatch(schema);

        // cost: 39ms
        // ans size: 3673
        // AFTER_MATCH_SKIP_TO_NEXT_ROW
        List<String> ans = nfa.getMatchResults(schema, SelectionStrategy.SKIP_TILL_ANY_MATCH);
        for(String str : ans){
            System.out.println(str);
        }

        List<byte[]> records0 = nfa.getProjectedRecords("A");
        System.out.println("Projection A");
        for(byte[] record : records0){
            System.out.println(schema.getRecordStr(record));
        }

        System.out.println("Projection B");
        List<byte[]> records1 = nfa.getProjectedRecords("B");
        for(byte[] record : records1){
            System.out.println(schema.getRecordStr(record));
        }

        System.out.println("Projection C");
        List<byte[]> records2 = nfa.getProjectedRecords("C");
        for(byte[] record : records2){
            System.out.println(schema.getRecordStr(record));
        }
        System.out.println("ans size: " + ans.size());
    }

//    @Test
//    public void testCitibike(){
//        String[] records = {
//
//        };
//        EventSchema schema = EventSchema.getEventSchema("CITIBIKE");
//        List<byte[]> byteRecords = new ArrayList<>(records.length);
//        for(String record : records){
//            byteRecords.add(schema.covertStringToBytes(record));
//        }
//        String sql =
//                "SELECT * FROM citibike MATCH_RECOGNIZE(\n" +
//                "    ORDER BY eventTime\n" +
//                "    MEASURES A.ride_id as AID, B.ride_id as BID, C.ride_id AS CID\n" +
//                "    ONE ROW PER MATCH\n" +
//                "    AFTER MATCH SKIP TO NEXT ROW \n" +
//                "    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '30' MINUTE \n" +
//                "    DEFINE \n" +
//                "    A AS A.type = 'B', \n" +
//                "    B AS B.type = 'F' AND B.start_station_id = A.end_station_id, \n" +
//                "    C AS C.type = 'J' AND C.start_station_id = B.end_station_id\n" +
//                ") MR;";
//
//        QueryParse query = new QueryParse(sql);
//        NFA nfa = new NFA();
//        nfa.constructNFA(query);
//        for(byte[] record : byteRecords){
//            nfa.consume(record, SelectionStrategy.SKIP_TILL_ANY_MATCH, schema);
//        }
//        nfa.printAnyMatch(schema);
//    }
}