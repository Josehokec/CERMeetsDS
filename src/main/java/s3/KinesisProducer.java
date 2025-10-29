package s3;

import compute.EvaluationEngineFlink;
import org.apache.flink.table.api.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.table.api.Schema;

public class KinesisProducer {

    public static void runLocal(){
        long startTime = System.currentTimeMillis();
        String tableName = "my_table";
        String sql =
                "SELECT * FROM my_table " +
                        "MATCH_RECOGNIZE ( " +
                        "  PARTITION BY name " +
                        "  ORDER BY eventTime " +
                        "  MEASURES " +
                        "    FIRST(A.eventTime) AS start_time, " +
                        "    LAST(C.eventTime) AS end_time, " +
                        "    AVG(A.sales) AS avg_sales_start, " +
                        "    AVG(C.sales) AS avg_sales_end, " +
                        "    (LAST(C.sales) - FIRST(A.sales)) AS sales_increase " +
                        "  ONE ROW PER MATCH " +
                        "  AFTER MATCH SKIP TO LAST C " +
                        "  PATTERN (A B C) " +
                        "  DEFINE " +
                        "    A AS A.sales > 0, " +
                        "    B AS B.sales > A.sales, " +
                        "    C AS C.sales > B.sales " +
                        ")";
        int testEventNum = 5000;
        List<DemoEvent> events = new ArrayList<>(testEventNum);
        String[] names = new String[] {"Alice", "Bob", "Candy"};
        Random random = new Random(7);
        for(int i = 0; i < testEventNum; i++){

            String name = names[random.nextInt(3)];
            int baseSales;
            switch(name) {
                case "Alice":
                    baseSales = 100 + (i < 300 ? random.nextInt(200) : random.nextInt(50));
                    break;
                case "Bob":
                    baseSales = 80 + (int)(i * 0.1) + random.nextInt(100);
                    break;
                case "Candy":
                    baseSales = 90 + random.nextInt(150);
                    break;
                default:
                    baseSales = 100;
            }

            events.add(new DemoEvent(name, i * 100, baseSales,
                    "P" + (random.nextInt(5) + 1), "Region" + (random.nextInt(3) + 1)));
        }

        Schema schema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("eventTime", DataTypes.TIMESTAMP(3))
                .column("sales", DataTypes.INT())
                .column("product_id", DataTypes.STRING())
                .column("region", DataTypes.STRING())
                .watermark("eventTime", "eventTime - INTERVAL '1' SECOND")
                .build();

        EvaluationEngineFlink.processQuery(events, schema, sql, tableName);

        long endTime = System.currentTimeMillis();
        System.out.println("match cost: " + (endTime - startTime) + "ms");
    }

    public static void main(String[] args) {
        runLocal();
    }
}


/*
package s3;

import compute.EvaluationEngineFlink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;


// 1. 定义发送的消息体结构
class SqlRequestPayload {
    public String id;
    public String sql;          // 例如: "SELECT name, age FROM inputTable WHERE age > 18"
    public String tableName;    // 例如: "inputTable"
    public List<Map<String, Object>> data; // 原始数据，例如: [{"name":"Alice", "age":20}, ...]
    // 注意：Schema 在这里简化处理，实际场景中你可能需要定义字段名和类型映射
}

public class KinesisProducer {

    public static void demoTest(int id){
        long startTime = System.currentTimeMillis();
        KinesisClient kinesisClient = KinesisClient.builder()
                .region(Region.US_EAST_1) // 替换你的 Region
                .build();

        // 模拟构建数据
        SqlRequestPayload payload = new SqlRequestPayload();
        payload.id = "req_" + id;
        payload.tableName = "my_table";
        payload.sql = "SELECT name, count(*) as cnt FROM my_table GROUP BY name";

        payload.data = new ArrayList<>(1000);
        String[] names = new String[] {"Alice", "Bob", "Candy"};
        Random random = new Random(7);
        for(int i = 0; i < 10000; i++){
            Object obj = i;
            payload.data.add(new HashMap<String, Object>() {{
                put("name", names[random.nextInt(3)]);
                put("val", obj);
            }});
        }

        Gson gson = new Gson();
        String jsonPayload = gson.toJson(payload);

        PutRecordRequest request = PutRecordRequest.builder()
                .streamName("SqlRequestStream")
                .partitionKey(payload.id)
                .data(SdkBytes.fromString(jsonPayload, StandardCharsets.UTF_8))
                .build();

        kinesisClient.putRecord(request);
        // System.out.println("Sent: " + jsonPayload);
        System.out.println("string length: " + jsonPayload.length());
        long endTime = System.currentTimeMillis();
        System.out.println("send cost: " + (endTime - startTime) + "ms");
    }

    public static void demoTestCER(int id){
        long startTime = System.currentTimeMillis();
        KinesisClient kinesisClient = KinesisClient.builder()
                .region(Region.US_EAST_1) // 替换你的 Region
                .build();

        // 模拟构建数据
        SqlRequestPayload payload = new SqlRequestPayload();
        payload.id = "req_" + id;
        payload.tableName = "my_table";
        payload.sql =
                "SELECT * FROM my_table " +
                "MATCH_RECOGNIZE ( " +
                "  PARTITION BY name " +
                "  ORDER BY EVENTTIME " +
                "  MEASURES " +
                "    FIRST(A.EVENTTIME) AS start_time, " +
                "    LAST(C.EVENTTIME) AS end_time, " +
                "    AVG(A.sales) AS avg_sales_start, " +
                "    AVG(C.sales) AS avg_sales_end, " +
                "    (LAST(C.sales) - FIRST(A.sales)) AS sales_increase " +
                "  ONE ROW PER MATCH " +
                "  AFTER MATCH SKIP TO LAST C " +
                "  PATTERN (A B C) " +
                "  DEFINE " +
                "    A AS A.sales > 0, " +
                "    B AS B.sales > A.sales, " +
                "    C AS C.sales > B.sales " +
                ")";
        int testEventNum = 50_000;
        payload.data = new ArrayList<>(testEventNum);
        String[] names = new String[] {"Alice", "Bob", "Candy"};
        Random random = new Random(7);
        for(int i = 0; i < testEventNum; i++){
            Map<String, Object> record = new HashMap<>();
            String name = names[random.nextInt(3)];
            int baseSales;
            switch(name) {
                case "Alice":
                    baseSales = 100 + (i < 300 ? random.nextInt(200) : random.nextInt(50));
                    break;
                case "Bob":
                    baseSales = 80 + (int)(i * 0.1) + random.nextInt(100);
                    break;
                case "Candy":
                    baseSales = 90 + random.nextInt(150);
                    break;
                default:
                    baseSales = 100;
            }

            record.put("name", name);

            record.put("EVENTTIME", i * 100);

            record.put("sales", baseSales);
            record.put("product_id", "P" + (random.nextInt(5) + 1));
            record.put("region", "Region" + (random.nextInt(3) + 1));

            payload.data.add(record);
        }

        Gson gson = new Gson();
        String jsonPayload = gson.toJson(payload);

        PutRecordRequest request = PutRecordRequest.builder()
                .streamName("SqlRequestStream")
                .partitionKey(payload.id)
                .data(SdkBytes.fromString(jsonPayload, StandardCharsets.UTF_8))
                .build();

        kinesisClient.putRecord(request);
        //System.out.println("Sent: " + jsonPayload);
        long endTime = System.currentTimeMillis();
        System.out.println("[remote] string length: " + jsonPayload.length() + " send cost: " + (endTime - startTime) + "ms");
    }

    public static void runLocal(){
        long startTime = System.currentTimeMillis();
        String tableName = "my_table";
        String sql =
                "SELECT * FROM my_table " +
                        "MATCH_RECOGNIZE ( " +
                        "  PARTITION BY name " +
                        "  ORDER BY eventTime " +
                        "  MEASURES " +
                        "    FIRST(A.eventTime) AS start_time, " +
                        "    LAST(C.eventTime) AS end_time, " +
                        "    AVG(A.sales) AS avg_sales_start, " +
                        "    AVG(C.sales) AS avg_sales_end, " +
                        "    (LAST(C.sales) - FIRST(A.sales)) AS sales_increase " +
                        "  ONE ROW PER MATCH " +
                        "  AFTER MATCH SKIP TO LAST C " +
                        "  PATTERN (A B C) " +
                        "  DEFINE " +
                        "    A AS A.sales > 0, " +
                        "    B AS B.sales > A.sales, " +
                        "    C AS C.sales > B.sales " +
                        ")";
        int testEventNum = 50_000;
        List<DemoEvent> events = new ArrayList<>(testEventNum);
        String[] names = new String[] {"Alice", "Bob", "Candy"};
        Random random = new Random(7);
        for(int i = 0; i < testEventNum; i++){

            String name = names[random.nextInt(3)];
            int baseSales;
            switch(name) {
                case "Alice":
                    baseSales = 100 + (i < 300 ? random.nextInt(200) : random.nextInt(50));
                    break;
                case "Bob":
                    baseSales = 80 + (int)(i * 0.1) + random.nextInt(100);
                    break;
                case "Candy":
                    baseSales = 90 + random.nextInt(150);
                    break;
                default:
                    baseSales = 100;
            }

            events.add(new DemoEvent(name, i * 100, baseSales,
                    "P" + (random.nextInt(5) + 1), "Region" + (random.nextInt(3) + 1)));
        }

        Schema schema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("eventTime", DataTypes.TIMESTAMP(3))
                .column("sales", DataTypes.INT())
                .column("product_id", DataTypes.STRING())
                .column("region", DataTypes.STRING())
                .watermark("eventTime", "eventTime - INTERVAL '1' SECOND")
                .build();

        EvaluationEngineFlink.processQuery(events, schema, sql, tableName);

        long endTime = System.currentTimeMillis();
        System.out.println("match cost: " + (endTime - startTime) + "ms");
    }

    public static void callKinesis(int id, String sql, List<Map<String, Object>> data, String dataName){
        long startTime = System.currentTimeMillis();
        KinesisClient kinesisClient = KinesisClient.builder()
                .region(Region.US_EAST_1) // 替换你的 Region
                .build();
        SqlRequestPayload payload = new SqlRequestPayload();
        payload.id = "req_" + id;
        payload.tableName = dataName;
        payload.sql = sql;
        payload.data = data;

        Gson gson = new Gson();
        String jsonPayload = gson.toJson(payload);
        PutRecordRequest request = PutRecordRequest.builder()
                .streamName("SqlRequestStream")
                .partitionKey(payload.id)
                .data(SdkBytes.fromString(jsonPayload, StandardCharsets.UTF_8))
                .build();

        kinesisClient.putRecord(request);
        //System.out.println("Sent: " + jsonPayload);
        System.out.println("string length: " + jsonPayload.length());
        long endTime = System.currentTimeMillis();
        System.out.println("send cost: " + (endTime - startTime) + "ms");
    }

    public static void main(String[] args) {
        // demoTest(i);

        for(int i = 0; i < 30; i++){
            demoTestCER(100000 + i);

            runLocal();
        }


    }
}

<!-->kinesis test<-->
        <dependency>
            <groupId>software.amazon.awssdk</groupId>
            <artifactId>kinesis</artifactId>
            <version>2.20.0</version>
        </dependency>
        <dependency>
            <groupId>com.google.code.gson</groupId>
            <artifactId>gson</artifactId>
            <version>2.10.1</version>
        </dependency>
 */