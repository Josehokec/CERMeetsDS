//package serverless;
//
//
///*
//架构前提
//你需要预先在 AWS 上创建以下资源：
//1. Input Stream (Kinesis): 用于接收 EC2 发来的源数据。
//2. Output Stream (Kinesis): 用于接收 Flink 处理后的结果，EC2 将监听这个流以获取结果。
//3. KDA Application: 一个空的 Flink 应用框架，上传下文中的 Flink Jar 包。
//https://console.aws.amazon.com/kinesis
//
//Create data stream
//Data stream name: input-stream-ec2-source
//容量模式: 按需
//最大记录大小：2048 KiB
// */
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import software.amazon.awssdk.core.SdkBytes;
//import software.amazon.awssdk.services.kinesis.KinesisClient;
//import software.amazon.awssdk.services.kinesis.model.*;
//import software.amazon.awssdk.services.kinesisanalyticsv2.KinesisAnalyticsV2Client;
//import software.amazon.awssdk.services.kinesisanalyticsv2.model.*;
//
//import java.nio.charset.StandardCharsets;
//import java.util.*;
//
//public class Ec2JobOrchestrator {
//
//    private static final String INPUT_STREAM = "input-stream-ec2-source";
//    private static final String OUTPUT_STREAM = "output-stream-flink-results";
//    private static final String APP_NAME = "MyFlinkSQK";
//    private static final String REGION = "us-east-1";
//
//    private final KinesisClient kinesisClient;
//    private final KinesisAnalyticsV2Client kdaClient;
//    private final ObjectMapper objectMapper;
//
//    public Ec2JobOrchestrator() {
//        this.kinesisClient = KinesisClient.create();
//        this.kdaClient = KinesisAnalyticsV2Client.create();
//        this.objectMapper = new ObjectMapper();
//    }
//
//    /**
//     * 主流程入口
//     * @param rawData EC2 上过滤好的原始字节数据
//     * @param userSql 用户编写的 MATCH_RECOGNIZE SQL (DML)
//     * @param schemaDDL 对应当前数据结构的 CREATE TABLE 语句 (DDL)
//     */
//    public void processBytes(List<byte[]> rawData, String userSql, String schemaDDL) throws Exception {
//
//        // 1. 数据预处理：将 byte[] 转换为 JSON 字符串列表
//        // 假设 byte[] 是某种对象的序列化，你需要先反序列化成对象，再转 JSON
//        // 这里简化演示：假设 byte[] 本身就是 JSON 的字节
//        List<String> jsonDataList = new ArrayList<>();
//        for (byte[] b : rawData) {
//            // 如果 byte[] 是 Java Object 序列化，需在此处反序列化为 Object，再用 objectMapper.writeValueAsString(obj)
//            jsonDataList.add(new String(b, StandardCharsets.UTF_8));
//        }
//
//        // 2. 配置 Flink 应用：注入 SQL 和 DDL
//        updateFlinkConfiguration(userSql, schemaDDL);
//
//        // 3. 启动 Flink 应用 (如果已运行则可能需要重启以应用新配置，或者设计为热加载)
//        startFlinkApplication();
//
//        // 4. 发送数据到 Input Stream
//        sendDataToKinesis(jsonDataList);
//
//        // 5. (可选) 监听 Output Stream 获取结果
//        // 注意：这是一个阻塞操作，通常建议异步处理
//        monitorResults();
//    }
//
//    private void updateFlinkConfiguration(String sql, String ddl) {
//        System.out.println("正在更新 Flink SQL 配置...");
//
//        // 获取当前应用版本
//        DescribeApplicationResponse descParams = kdaClient.describeApplication(
//                DescribeApplicationRequest.builder().applicationName(APP_NAME).build());
//        long currentVersion = descParams.applicationDetail().applicationVersionId();
//
//        // 构建属性组，通过 EnvironmentProperties 传递给 Flink
//        Map<String, String> sqlConfig = new HashMap<>();
//        sqlConfig.put("UserSQL", sql);
//        sqlConfig.put("InputDDL", ddl);
//        // 定义输出表的 DDL，通常结构比较固定，或者是简单的 String 结果
//        String outputDDL = "CREATE TABLE output_table (result STRING) WITH ('connector'='kinesis', 'stream'='" + OUTPUT_STREAM + "', 'format'='json', 'aws.region'='" + REGION + "')";
//        sqlConfig.put("OutputDDL", outputDDL);
//
//        PropertyGroup propGroup = PropertyGroup.builder()
//                .propertyGroupId("DynamicLogic")
//                .propertyMap(sqlConfig)
//                .build();
//
//        kdaClient.updateApplication(UpdateApplicationRequest.builder()
//                .applicationName(APP_NAME)
//                .currentApplicationVersionId(currentVersion)
//                .applicationConfigurationUpdate(ApplicationConfigurationUpdate.builder()
//                        .environmentPropertyUpdates(EnvironmentPropertyUpdates.builder()
//                                .propertyGroups(propGroup).build())
//                        .build())
//                .build());
//    }
//
//    private void startFlinkApplication() {
//        try {
//            DescribeApplicationResponse resp = kdaClient.describeApplication(
//                    DescribeApplicationRequest.builder().applicationName(APP_NAME).build());
//
//            ApplicationStatus status = resp.applicationDetail().applicationStatus();
//
//            if (status == ApplicationStatus.READY) {
//                System.out.println("启动 Flink 应用...");
//                kdaClient.startApplication(StartApplicationRequest.builder()
//                        .applicationName(APP_NAME)
//                        .runConfiguration(RunConfiguration.builder().build())
//                        .build());
//            } else if (status == ApplicationStatus.RUNNING) {
//                // 如果已经在运行，且我们使用的是 PropertyGroup 热更新机制，可能不需要重启
//                // 但如果逻辑变化巨大，建议 Stop 然后 Start
//                System.out.println("应用正在运行。注意：如果需要立即生效新逻辑，可能需要重启策略。");
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    private void sendDataToKinesis(List<String> jsonEvents) {
//        System.out.println("正在发送 " + jsonEvents.size() + " 条数据到 Kinesis...");
//        for (String event : jsonEvents) {
//            kinesisClient.putRecord(PutRecordRequest.builder()
//                    .streamName(INPUT_STREAM)
//                    .data(SdkBytes.fromUtf8String(event))
//                    .partitionKey(UUID.randomUUID().toString())
//                    .build());
//        }
//    }
//
//    private void monitorResults() {
//        System.out.println("开始监听结果流...");
//        // 获取 Shard Iterator
//        ListShardsResponse shards = kinesisClient.listShards(ListShardsRequest.builder().streamName(OUTPUT_STREAM).build());
//        String shardId = shards.shards().get(0).shardId(); // 简化：只监听第一个分片
//
//        String shardIterator = kinesisClient.getShardIterator(GetShardIteratorRequest.builder()
//                .streamName(OUTPUT_STREAM)
//                .shardId(shardId)
//                .shardIteratorType(ShardIteratorType.LATEST)
//                .build()).shardIterator();
//
//        // 循环拉取数据 (简单的轮询实现)
//        for (int i = 0; i < 10; i++) { // 仅演示拉取 10 次
//            GetRecordsResponse recordsResp = kinesisClient.getRecords(GetRecordsRequest.builder()
//                    .shardIterator(shardIterator)
//                    .limit(10)
//                    .build());
//
//            for (Record record : recordsResp.records()) {
//                String resultData = record.data().asUtf8String();
//                System.out.println("收到 Flink 计算结果: " + resultData);
//            }
//
//            shardIterator = recordsResp.nextShardIterator();
//            try { Thread.sleep(1000); } catch (InterruptedException e) {}
//        }
//    }
//
//    public static void main(String[] args) throws Exception {
//        Ec2JobOrchestrator orchestrator = new Ec2JobOrchestrator();
//
//        // 示例输入数据 (byte[])
//        List<byte[]> rawData = new ArrayList<>();
//        rawData.add("{\"crime_id\":\"c1\",\"district\":\"001\",\"action_time\":\"2023-12-01T12:00:00.000Z\"}".getBytes(StandardCharsets.UTF_8));
//        rawData.add("{\"crime_id\":\"c2\",\"district\":\"001\",\"action_time\":\"2023-12-01T12:00:05.000Z\"}".getBytes(StandardCharsets.UTF_8));
//        // 额外事件（不同 district，不会与上面匹配），以及一个后续时间的同 district 事件，可产生更多匹配
//        rawData.add("{\"crime_id\":\"c3\",\"district\":\"002\",\"action_time\":\"2023-12-01T12:01:00.000Z\"}".getBytes(StandardCharsets.UTF_8));
//        rawData.add("{\"crime_id\":\"c4\",\"district\":\"001\",\"action_time\":\"2023-12-01T12:00:10.000Z\"}".getBytes(StandardCharsets.UTF_8));
//
//
//        String userSql = "SELECT *\n" +
//                "FROM source_stream\n" +
//                "MATCH_RECOGNIZE (\n" +
//                "    PARTITION BY district\n" +
//                "    ORDER BY action_time\n" +
//                "    MEASURES A.crime_id AS first_crime, B.crime_id AS second_crime\n" +
//                "    ONE ROW PER MATCH\n" +
//                "    PATTERN (A B)\n" +
//                "    DEFINE\n" +
//                "        A AS A.district = '001',\n" +
//                "        B AS B.district = '001' AND B.action_time > A.action_time\n" +
//                ")";
//        String schemaDDL = "CREATE TABLE source_stream (\n" +
//                "    crime_id STRING,\n" +
//                "    district STRING,\n" +
//                "    action_time TIMESTAMP(3),\n" +
//                "    WATERMARK FOR action_time AS action_time - INTERVAL '5' SECOND\n" +
//                ") WITH (\n" +
//                "    'connector' = 'kinesis',\n" +
//                "    'stream' = 'input-stream-ec2-source',\n" +
//                "    'aws.region' = 'us-east-1',\n" +
//                "    'format' = 'json',\n" +
//                "    'scan.stream.initpos' = 'LATEST'\n" +
//                ")";
//        orchestrator.processBytes(rawData, userSql, schemaDDL);
//    }
//}
