package compute;

import event.CitibikeEvent;
import event.ClusterEvent;
import event.CrimesEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import store.EventSchema;
import utils.SortByTs;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EvaluationEngineFlink {

    public static <T> void processQuery(List<T> events, Schema schema, String sql, String tableName) {
        long startTime = System.currentTimeMillis();
        EnvironmentSettings settings;
        StreamTableEnvironment tEnv = null;
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            tEnv = StreamTableEnvironment.create(env, settings);
            DataStream<T> dataStream = env.fromCollection(events);
            Table table = tEnv.fromDataStream(dataStream, schema);
            tEnv.createTemporaryView(tableName, table);
        } catch (Exception e) {
            e.printStackTrace();
        }

        TableResult res = tEnv.executeSql(sql);

        try (CloseableIterator<Row> iterator = res.collect()) {
            int rowCount = 0;
            while (iterator.hasNext()) {
//                Row row = iterator.next();
//                for (int i = 0; i < row.getArity(); i++) {
//                    System.out.print(row.getField(i) + " ");
//                }
//                System.out.println();
                iterator.next();
                rowCount++;
            }
            System.out.println("number of tuples: " + rowCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        tEnv.dropTemporaryView(tableName);
        long endTime = System.currentTimeMillis();
        System.out.println("match time: " + (endTime - startTime) + "ms");
    }


    public static void main(String[] args) {
        testCrimesDemo();
    }

    public static  void testCrimesDemo(){
        String[] lines = {
                "ROBBERY,13152945,JG355457,0331,821,8,41.813803157,-87.704715262,03,1690288200",
                "BATTERY,13153551,JG355405,0486,2535,25,41.90998664,-87.717383916,08B,1690288800",
                "ROBBERY,13152717,JG355396,031A,725,7,41.769728518,-87.662736227,03,1690288980",
                "MOTOR_VEHICLE_THEFT,13154279,JG357351,0917,1611,16,42.003370192,-87.812289827,07,1690289100",
                "BATTERY,13154880,JG356918,0430,234,2,41.795190552,-87.588007653,04B,1690290000",
                "MOTOR_VEHICLE_THEFT,13152868,JG355772,0910,414,4,41.755173789,-87.581760293,07,1690290000",
                "ROBBERY,13152748,JG355475,033A,1532,15,41.888460669,-87.755363176,03,1690292640",
                "BATTERY,13153604,JG355513,0486,331,3,41.772540956,-87.574024077,08B,1690293600",
                "MOTOR_VEHICLE_THEFT,13155195,JG358180,0910,1032,10,41.837093606,-87.709712948,07,1690293600"
        };

        List<CrimesEvent> events = new ArrayList<>(10);

        for(String line : lines){
            CrimesEvent event = new CrimesEvent(line);
            System.out.println("event: " + event);
            events.add(event);
        }

        Schema schema = Schema.newBuilder()
                .column("type", DataTypes.STRING())
                .column("id", DataTypes.INT())
                .column("caseNumber", DataTypes.STRING())
                .column("IUCR", DataTypes.STRING())
                .column("beat", DataTypes.INT())
                .column("district", DataTypes.INT())
                .column("latitude", DataTypes.DOUBLE())
                .column("longitude", DataTypes.DOUBLE())
                .column("FBICode", DataTypes.STRING())
                .column("eventTime", DataTypes.TIMESTAMP(3))
                .watermark("eventTime", "eventTime - INTERVAL '1' SECOND")
                .build();
        // "eventTime - INTERVAL '1' SECOND"
        String sql =
                "SELECT * FROM CRIMES MATCH_RECOGNIZE(\n" +
                        "    ORDER BY eventTime\n" +
                        "    MEASURES A.id as AID, B.id as BID, C.id AS CID\n" +
                        "    ONE ROW PER MATCH\n" +
                        "    AFTER MATCH SKIP TO NEXT ROW \n" +
                        "    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '30' MINUTE \n" +
                        "    DEFINE \n" +
                        "        A AS A.type = 'ROBBERY' , \n" + // AND A.beat >= 1900 AND A.beat <= 2000
                        "        B AS B.type = 'BATTERY', \n" +
                        "        C AS C.type = 'MOTOR_VEHICLE_THEFT' \n" +  //AND C.district = B.district
                        ") MR;";

        for(int loop = 0; loop < 10; loop++){
            long startTime = System.currentTimeMillis();
            processQuery(events, schema, sql, "CRIMES");
            long endTime = System.currentTimeMillis();
            System.out.println("cost time: " + (endTime - startTime) + "ms");
        }
    }
//
//
//    public static void testCitibikeDemo(){
//        String[] lines = {
//                "F,6303933948432017339,520701,520701,40.71273266,-74.0046073,40.71273266,-74.0046073,1672585011195",
//                "B,-8414818342391600025,520701,520701,40.71273266,-74.0046073,40.71273266,-74.0046073,1672584570948",
//                "J,7154601157927936014,520701,482106,40.71273266,-74.0046073,40.69991755,-73.98971773,1672585864298",
//                "F,3562729827234984498,370404,370404,40.663657,-73.963014,40.663657,-73.963014,1672588848654",
//                "B,7395667951823775349,370404,370404,40.663657,-73.963014,40.663657,-73.963014,1672587259244",
//                "J,-1443849945380959807,370404,370404,40.663657,-73.963014,40.663657,-73.963014,1672588855219"
//        };
//        List<byte[]> byteRecords = new ArrayList<>(10);
//
//        EventSchema eventSchema = EventSchema.getEventSchema("CITIBIKE");
//        for(String line : lines){
//            byte[] record = eventSchema.covertStringToBytes(line);
//            byteRecords.add(record);
//        }
//        byteRecords = SortByTs.sort(byteRecords, eventSchema);
//        List<CitibikeEvent> events = byteRecords.stream().map(CitibikeEvent::valueOf).collect(Collectors.toList());
//
//        Schema schema = Schema.newBuilder()
//                .column("type", DataTypes.STRING())
//                .column("ride_id", DataTypes.BIGINT())
//                .column("start_station_id", DataTypes.INT())
//                .column("end_station_id", DataTypes.INT())
//                .column("start_lat", DataTypes.DOUBLE())
//                .column("start_lng", DataTypes.DOUBLE())
//                .column("end_lat", DataTypes.DOUBLE())
//                .column("end_lng", DataTypes.DOUBLE())
//                .column("eventTime", DataTypes.TIMESTAMP(3))
//                .watermark("eventTime", "eventTime - INTERVAL '1' SECOND")
//                .build();
//
//        String sql =
//                "SELECT * FROM citibike MATCH_RECOGNIZE(\n" +
//                        "    ORDER BY eventTime\n" +
//                        "    MEASURES A.ride_id as AID, B.ride_id as BID, C.ride_id AS CID\n" +
//                        "    ONE ROW PER MATCH\n" +
//                        "    AFTER MATCH SKIP TO NEXT ROW \n" +
//                        "    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '30' MINUTE \n" +
//                        "    DEFINE \n" +
//                        "    A AS A.type = 'B', \n" +
//                        "    B AS B.type = 'F' AND B.start_station_id = A.end_station_id, \n" +
//                        "    C AS C.type = 'J' AND C.start_station_id = B.end_station_id\n" +
//                        ") MR;";
//
//        for(int i = 0; i < 10; i++){
//            processQuery(events, schema, sql, "citibike");
//        }
//    }
//
//
//    public static void testClusterDemo(){
//        //ClusterEvent: TP0, 6277853776, 374, 0, 0, 0.0, 9.537E-7, 0.0, 1970-01-04T04:59:21.502
//        //ClusterEvent: TP1, 6277853776, 374, 0, 0, 0.0, 9.537E-7, 0.0,1970-01-04T04:59:24.478
//        //ClusterEvent: TP4, 6280586944,   0, 0, 1, 0.0, 1.554E-4, 0.0, 1970-01-04T04:59:26.142
//
//        //ClusterEvent: TP0,6319614391,0,0,1,0.0,1.554E-4,0.0,1970-01-10T05:53:30.505
//        //ClusterEvent: TP1,6319614391,0,0,1,0.0,1.554E-4,0.0,1970-01-10T05:53:30.505
//        //ClusterEvent: TP4,6319614391,0,0,1,0.0,1.554E-4,0.0,1970-01-10T05:53:30.505
//        String[] lines = {
//                //"TP0,6277853776,374,0,0,0.0,9.537e-07,0.0,248361502729", //real
//                "TP0,6277853776,374,0,0,0.0,9.537,0.0,248362502729",
//                "TP1,6277853776,374,0,0,0.0,9.537,0.0,248364478646",
//                "TP4,6277853776,0,0,1,0.0,1.554,0.0,248366142283",
//        };
//        List<byte[]> byteRecords = new ArrayList<>(10);
//
//        EventSchema eventSchema = EventSchema.getEventSchema("cluster");
//        for(String line : lines){
//            byte[] record = eventSchema.covertStringToBytes(line);
//            byteRecords.add(record);
//        }
//        List<ClusterEvent> events = byteRecords.stream().map(ClusterEvent::valueOf).collect(Collectors.toList());
//
//        for(ClusterEvent event : events){
//            System.out.println(event);
//        }
//
//        Schema schema = Schema.newBuilder()
//                .column("type", DataTypes.STRING())
//                .column("JOBID", DataTypes.BIGINT())
//                .column("index", DataTypes.INT())
//                .column("scheduling", DataTypes.STRING())
//                .column("priority", DataTypes.INT())
//                .column("CPU", DataTypes.FLOAT())
//                .column("RAM", DataTypes.FLOAT())
//                .column("DISK", DataTypes.FLOAT())
//                .column("eventTime", DataTypes.TIMESTAMP(3))
//                .watermark("eventTime", "eventTime - INTERVAL '1' SECOND")
//                .build();
//
//        // serious bug....
//        String sql = "SELECT * FROM cluster MATCH_RECOGNIZE(\n" +
//                "    ORDER BY eventTime\n" +
//                "    MEASURES A.eventTime as ATS, B.eventTime as BTS, C.eventTime AS CTS\n" +
//                "    ONE ROW PER MATCH\n" +
//                "    AFTER MATCH SKIP TO NEXT ROW \n" +
//                "    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '10' HOUR \n" +
//                "    DEFINE \n" +
//                "        A AS A.type = 'TP0',\n" +
//                "        B AS B.type = 'TP1' AND A.JOBID = B.JOBID,\n" +
//                "        C AS C.type = 'TP4' AND C.JOBID = A.JOBID AND B.JOBID = C.JOBID\n" +
//                ");";
//        processQuery(events, schema, sql, "cluster");
//    }
}
