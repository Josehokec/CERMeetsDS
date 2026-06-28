//package s3;
//
//import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
//import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
//import software.amazon.awssdk.core.async.AsyncResponseTransformer;
//import software.amazon.awssdk.regions.Region;
//import software.amazon.awssdk.services.s3.S3AsyncClient;
//import software.amazon.awssdk.services.s3.model.GetObjectRequest;
//import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
//import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
//
//import parser.IndependentPredicate;
//import store.ColumnInfo;
//import store.EventSchema;
//import store.EventStore;
//import utils.Pair;
//
//import java.net.URI;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.CopyOnWriteArrayList;
//import java.util.stream.Collectors;
//
//
//public class S3AsyncReader {
//    private final S3AsyncClient s3AsyncClient;
//
//    Map<String, List<String>> ipStringMap;
//    private final EventSchema schema;
//    Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap;
//
//    public S3AsyncReader(String accessKey, String secretKey, String region, Map<String, List<String>> ipStringMap, String tableName) {
//        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);
//        this.s3AsyncClient = S3AsyncClient.crtBuilder()
//                .region(Region.of(region))
//                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
//                .endpointOverride(URI.create("http://s3." + region + ".amazonaws.com"))
//                .targetThroughputInGbps(20.0)
//                .minimumPartSizeInBytes(8 * 1024 * 1024L)
//                .build();
//
//        this.ipStringMap = ipStringMap;
//        this.schema = EventSchema.getEventSchema(tableName);
//        ipMap = parseIpString(ipStringMap, schema);
//    }
//
//    // parse independent predicates
//    public static Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> parseIpString(Map<String, List<String>> ipStringMap, EventSchema schema) {
//        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = new HashMap<>(ipStringMap.size() << 2);
//        // parse independent predicates
//        for(String varName : ipStringMap.keySet()){
//            List<String> ipStringList = ipStringMap.get(varName);
//            List<Pair<IndependentPredicate, ColumnInfo>> pairs = new ArrayList<>(ipStringList.size());
//            for (String ipString : ipStringList){
//                IndependentPredicate ip = new IndependentPredicate(ipString);
//                pairs.add(new Pair<>(ip, schema.getColumnInfo(ip.getAttributeName())));
//            }
//            ipMap.put(varName, pairs);
//        }
//        return ipMap;
//    }
//
//    private CompletableFuture<List<byte[]>> downloadRangeAsync(String bucketName, String key, long start, long end, int pageSize) {
//        String range = "bytes=" + start + "-" + end;
//        GetObjectRequest rangeRequest = GetObjectRequest.builder()
//                .bucket(bucketName)
//                .key(key)
//                .range(range)
//                .build();
//        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = parseIpString(ipStringMap, schema);
//        List<byte[]> filteredRecords = new CopyOnWriteArrayList<>();
//
//        return s3AsyncClient.getObject(rangeRequest, AsyncResponseTransformer.toBytes())
//                .thenApply(responseBytes -> {
//                    ByteBuffer bb = responseBytes.asByteBuffer();
//                    int totalBytes = bb.remaining();
//                    int dataLen = schema.getFixedRecordLen();
//                    int singlePageRecordNum = pageSize / dataLen;
//
//                    while (bb.remaining() >= pageSize) {
//                        int currentPageStart = bb.position();
//                        for (int i = 0; i < singlePageRecordNum; i++) {
//                            int recordOffset = currentPageStart + (i * dataLen);
//
//                            if (checkMatch(bb, recordOffset, ipMap)) {
//                                byte[] record = new byte[dataLen];
//                                // bb.get(recordOffset, record);
//                                bb.get(record, recordOffset, dataLen);
//                                filteredRecords.add(record);
//                            }
//                        }
//
//                        bb.position(currentPageStart + pageSize);
//                    }
//                    return filteredRecords;
//                });
//    }
//
//    private boolean checkMatch(ByteBuffer bb, int pos, Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap) {
//        for (List<Pair<IndependentPredicate, ColumnInfo>> ips : ipMap.values()) {
//            boolean satisfied = true;
//            for (Pair<IndependentPredicate, ColumnInfo> pair : ips) {
//                Object obj = schema.getColumnValue(pair.getValue(), bb, pos);
//                if (!pair.getKey().check(obj, pair.getValue().getDataType())) {
//                    satisfied = false;
//                    break;
//                }
//            }
//            if (satisfied) return true;
//        }
//        return false;
//    }
//
//    public List<byte[]> downloadFileParallel(String bucketName, String key, int threadCount, int pageSize) {
//        HeadObjectRequest headRequest = HeadObjectRequest.builder()
//                .bucket(bucketName)
//                .key(key)
//                .build();
//
//        HeadObjectResponse headResponse = s3AsyncClient.headObject(headRequest).join();
//        long totalSize = headResponse.contentLength();
//
//        System.out.printf("Total Size: %d bytes, Parallel Requests: %d, PageSize: %d bytes%n",
//                totalSize, threadCount, pageSize);
//
//        long rawPartSize = totalSize / threadCount;
//        long alignedPartSize = (rawPartSize / pageSize) * pageSize;
//        if (alignedPartSize == 0 && totalSize >= pageSize) {
//            alignedPartSize = pageSize;
//        }
//
//        List<CompletableFuture<List<byte[]>>> futures = new ArrayList<>();
//        long startTime = System.currentTimeMillis();
//        long currentStart = 0;
//
//        for (int i = 0; i < threadCount; i++) {
//            long end;
//            if (i == threadCount - 1) {
//                end = totalSize - 1;
//            } else {
//                end = currentStart + alignedPartSize - 1;
//            }
//
//            if (currentStart > end) break;
//
//            futures.add(downloadRangeAsync(bucketName, key, currentStart, end, pageSize));
//
//            currentStart = end + 1;
//        }
//
//        CompletableFuture<Void> allDone = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
//
//        List<byte[]> allRecords = allDone.thenApply(v -> futures.stream()
//                .map(CompletableFuture::join)
//                .flatMap(List::stream)
//                .collect(Collectors.toList())).exceptionally(ex -> {
//            System.err.println("Error during parallel download: " + ex.getMessage());
//            return new ArrayList<>();
//        }).join();
//
//        long endTime = System.currentTimeMillis();
//        System.out.println("Total execution time: " + (endTime - startTime) + " ms");
//        System.out.println("Total records found: " + allRecords.size());
//
//        return allRecords;
//    }
//
//
//    public static void main(String[] args) {
//        String accessKey = "accessKey";
//        String secretKey = "secretKey";
//        String region = "us-east-1";
//        String bucketName = "bucketName";
//
//        List<String> ips1 = new ArrayList<>();
//        ips1.add("V1.BEAT = 1024");//30189
//        ips1.add("V1.TYPE = 'ROBBERY'");//1216
//        Map<String, List<String>> ipStringMap = new HashMap<>();
//        ipStringMap.put("V1", ips1);
//
//        S3Reader downloader = new S3Reader(accessKey, secretKey, region, ipStringMap, "CRIMES");
//
//        List<byte[]> results = downloader.downloadFileParallel(bucketName, "CRIMES_0.store", 8, EventStore.pageSize);
//        System.out.println("Total records retrieved: " + results.size());
//    }
//}
//
