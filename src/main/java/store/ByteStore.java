package store;

import parser.QueryParse;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
convert csv file to byte file
 */
public class ByteStore {

    public static void storeCSVToByte(String filename, EventSchema schema){
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
        //String filePath = prefixPath + "dataset" + sep + "mini_" + filename + ".csv";
        String filePath = prefixPath + "dataset" + sep + filename + ".csv";
        EventStore store = new EventStore(filename, true);

        // store records into file
        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            // delete first line
            String line;
            b.readLine();
            while ((line = b.readLine()) != null) {
                byte[] record = schema.covertStringToBytes(line);
                store.insertSingleRecord(record, schema.getFixedRecordLen());
            }
            b.close();
            f.close();
            // force flush
            store.forceFlush();
        }catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void initialCrimes(){
        String sql = "create table crimes(" +
                "       Type VARCHAR(24), " +
                "       ID int, " +
                "       CaseNumber VARCHAR(9), " +
                "       IUCR VARCHAR(4), " +
                "       Beat int, " +
                "       District int, " +
                "       Latitude double, " +
                "       Longitude double, " +
                "       FBICode VARCHAR(3)," +
                "       eventTime long);";
        EventSchema schema = new EventSchema(sql);
        String filename = "crimes";

        System.out.println("start writing csv file to byte file...");
        long startTime = System.currentTimeMillis();
        ByteStore.storeCSVToByte(filename, schema);
        long endTime = System.currentTimeMillis();
        System.out.println("insertion cost " + (endTime - startTime) + "ms");
    }

    public static void initialCitibike(){
        // single file insertion cost 34768ms
        String sql = "create table citibike(" +
                "       type VARCHAR(1), " +
                "       ride_id long, " +
                "       start_station_id int, " +
                "       end_station_id int, " +
                "       start_lat double, " +
                "       start_lng double, " +
                "       end_lat double, " +
                "       end_lng double, " +
                "       eventTime long);";
        EventSchema schema = new EventSchema(sql);
        String filename = "citibike";

        System.out.println("start writing csv file...");
        long startTime = System.currentTimeMillis();
        ByteStore.storeCSVToByte(filename, schema);
        long endTime = System.currentTimeMillis();
        System.out.println("insertion cost " + (endTime - startTime) + "ms");
        //List<String> ips1 = new ArrayList<>();
        //ips1.add("V1.TYPE = 'A'");
        //ips1.add("V1.START_LAT <= 40.7");
        //ips1.add("V1.START_LAT >= 40.6");

        //List<String> ips2 = new ArrayList<>();
        //ips2.add("V2.TYPE = 'E'");

        //List<String> ips3 = new ArrayList<>();
        //ips3.add("V3.TYPE = 'I'");

        //Map<String, List<String>> ipStringMap = new HashMap<>();
        //ipStringMap.put("V1", ips1);
        //ipStringMap.put("V2", ips2);
        //ipStringMap.put("V3", ips3);

        //FullScan fullScan = new FullScan("CITIBIKE");

        //long startTime = System.currentTimeMillis();
        //EventCache cache = fullScan.concurrentScanBasedVarName(ipStringMap);
        //long endTime = System.currentTimeMillis();
        //System.out.println("query cost " + (endTime - startTime) + "ms");
        //System.out.println(cache.getCardinality());
        // query cost 6842ms vs. 2670ms
        //{V1=623646, V2=4646044, V3=3689987}
    }

    public static void initialCluster(){
        String sql = "create table cluster(" +
                "       Type VARCHAR(3), " +
                "       JobID long, " +
                "       Index int, " +
                "       Scheduling VARCHAR(1), " +
                "       Priority int, " +
                "       CPU float, " +
                "       RAM float, " +
                "       DISK float, " +
                "       eventTime long);";
        EventSchema schema = new EventSchema(sql);

        // please modify this filename...
        String filename = "cluster";
        System.out.println("start writing csv file to byte file...");
        long startTime = System.currentTimeMillis();
        ByteStore.storeCSVToByte(filename, schema);
        long endTime = System.currentTimeMillis();
        System.out.println("insertion cost " + (endTime - startTime) + "ms");
    }

    public static void initialSynthetic(){
        String sql = "create table SYNTHETIC(" +
                "       type VARCHAR(4)," +
                "       a1 int," +
                "       a2 VARCHAR(8)," +
                "       a3 VARCHAR(16)," +
                "       eventTime long);";

        // 50M cost 30s
        EventSchema schema = new EventSchema(sql);

        System.out.println("start writing csv file...");
        long startTime = System.currentTimeMillis();
        // please modify this filename...
        String filename = "synthetic50M";
        ByteStore.storeCSVToByte(filename, schema);
        long endTime = System.currentTimeMillis();
        System.out.println("insertion cost " + (endTime - startTime) + "ms");
        System.out.println("end writing csv file...");
    }

    public static void main(String[] args){
        /* initial function, please modify filename in the function */

        // initialCrimes();
        // initialCitibike();
        // initialCluster();
        // initialSynthetic();



//         List<String> ips1 = new ArrayList<>();
//         ips1.add("V1.BEAT = 1024");
//         Map<String, List<String>> ipStringMap = new HashMap<>();
//         ipStringMap.put("V1", ips1);

    }
}



/*

long startTime = System.currentTimeMillis();
FullScan fullScan = new FullScan("CRIMES");
int callNum = 0;
boolean finish = false;
List<byte[]> allRecords = new ArrayList<>();
while(!finish){
    List<byte[]> records =  fullScan.concurrentScan(callNum);
    if(records.isEmpty()){
        finish = true;
    }else{
        allRecords.addAll(records);
        callNum++;
    }
}
System.out.println("total records: " + allRecords.size());
long endTime = System.currentTimeMillis();
System.out.println("scan cost " + (endTime - startTime) + "ms");
EventSchema schema = EventSchema.getEventSchema("CRIMES");
for(int i = 0; i < 10; i++){
    byte[] record = allRecords.get(i);
    String recordStr = schema.getRecordStr(record);
    System.out.println(recordStr);
}



-------------------------------------------------
Cluster dataset
-------------------------------------------------
List<String> ips1 = new ArrayList<>();
ips1.add("V1.TYPE = 'TP7'");
Map<String, List<String>> ipStringMap = new HashMap<>();
ipStringMap.put("V1", ips1);

FullScan fullScan = new FullScan("CLUSTER");

long startTime = System.currentTimeMillis();
EventCache cache = fullScan.concurrentScanBasedVarName(ipStringMap);
long endTime = System.currentTimeMillis();
System.out.println("query cost " + (endTime - startTime) + "ms");
System.out.println(cache.getCardinality());




-------------------------------------------------
Crimes dataset
-------------------------------------------------
List<String> ips1 = new ArrayList<>();
ips1.add("V1.TYPE = 'ROBBERY'");

List<String> ips2 = new ArrayList<>();
ips2.add("V2.TYPE = 'BATTERY'");

List<String> ips3 = new ArrayList<>();
ips3.add("V3.TYPE = 'MOTOR_VEHICLE_THEFT'");
//V3=411886
ips3.add("V3.LATITUDE >= 39.08623357648542");
ips3.add("V3.LATITUDE <= 41.08623357648542");

Map<String, List<String>> ipStringMap = new HashMap<>();
ipStringMap.put("V1", ips1);
ipStringMap.put("V2", ips2);
ipStringMap.put("V3", ips3);

FullScan fullScan = new FullScan("CRIMES");

long start = System.currentTimeMillis();
EventCache cache = fullScan.concurrentScanBasedVarName(ipStringMap);
long end = System.currentTimeMillis();
System.out.println("query cost " + (end - start) + "ms");
System.out.println(cache.getCardinality());





String query = "SELECT * FROM CRIMES MATCH_RECOGNIZE(\n" +
                "    ORDER BY eventTime\n" +
                "    MEASURES A.id as AID, B.id as BID, C.id AS CID\n" +
                "    ONE ROW PER MATCH\n" +
                "    AFTER MATCH SKIP TO NEXT ROW\n" +
                "    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '30' MINUTE\n" +
                "    DEFINE\n" +
                "        A AS A.type = 'ROBBERY',\n" +
                "        B AS B.type = A.type,\n" +
                "        C AS C.type = 'MOTOR_VEHICLE_THEFT' AND C.beat >= 1611 AND C.beat <= 2211\n" +
                ");";
        QueryParse q = new QueryParse(query);
        Map<String, List<String>> ipStringMap = q.getIpStringMap();
        System.out.println(ipStringMap);


                 FullScan fullScan = new FullScan("CRIMES");

         long start = System.currentTimeMillis();
         EventCache cache = fullScan.concurrentScanBasedVarName(ipStringMap);
         long end = System.currentTimeMillis();
         System.out.println("query cost " + (end - start) + "ms");
         System.out.println(cache.getCardinality());
 */

