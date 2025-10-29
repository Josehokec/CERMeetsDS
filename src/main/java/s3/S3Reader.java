package s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import parser.IndependentPredicate;
import store.ColumnInfo;
import store.EventSchema;
import store.EventStore;
import utils.Pair;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class S3Reader {
    private final S3Client s3Client;

    Map<String, List<String>> ipStringMap;
    private final EventSchema schema;
    Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap;

    public S3Reader(Map<String, List<String>> ipStringMap, String tableName) {
        String accessKey = "your_key";
        String secretKey = "your_secret";
        String region = "us-east-1";

        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);
        this.s3Client = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .endpointOverride(URI.create("http://s3." + region + ".amazonaws.com"))   // http rather than https
                .build();
        this.ipStringMap = ipStringMap;
        this.schema = EventSchema.getEventSchema(tableName);
        ipMap = parseIpString(ipStringMap, schema);
    }

    public List<byte[]> downloadFileParallel(String fileName) {
        String bucketName = "yaya-s3-data";
        int threadCount = 8;
        int pageSize = EventStore.pageSize;
        HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();
        HeadObjectResponse headResponse = s3Client.headObject(headRequest);
        long totalSize = headResponse.contentLength();

        // System.out.printf("Total Size: %d bytes, Threads: %d, PageSize: %d bytes%n", totalSize, threadCount, pageSize);

        long rawPartSize = totalSize / threadCount;
        long alignedPartSize = (rawPartSize / pageSize) * pageSize;
        if (alignedPartSize == 0 && totalSize >= pageSize) {
            alignedPartSize = pageSize;
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        List<CompletableFuture<List<byte[]>>> futures = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        long currentStart = 0;

        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            long end;

            if (i == threadCount - 1) {
                end = totalSize - 1;
            } else {
                end = currentStart + alignedPartSize - 1;
            }

            if (currentStart > end) {
                System.out.println("Thread " + i + " skipped (No data left).");
                continue;
            }

            final long taskStart = currentStart;
            final long taskEnd = end;

            CompletableFuture<List<byte[]>> future = CompletableFuture.supplyAsync(() ->
                    downloadRangeAndFilter(bucketName, fileName, taskStart, taskEnd, pageSize, threadIndex), executor);

            futures.add(future);

            currentStart = taskEnd + 1;
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        List<byte[]> allRecords = new ArrayList<>();
        for (CompletableFuture<List<byte[]>> future : futures) {
            try {
                allRecords.addAll(future.join());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        long endTime = System.currentTimeMillis();
        //System.out.println("Total download time: " + (endTime - startTime) + " ms");
        //System.out.println("Total records found: " + allRecords.size());

        executor.shutdown();

        return allRecords;
    }

    // parse independent predicates
    public static Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> parseIpString(Map<String, List<String>> ipStringMap, EventSchema schema) {
        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = new HashMap<>(ipStringMap.size() << 2);
        // parse independent predicates
        for(String varName : ipStringMap.keySet()){
            List<String> ipStringList = ipStringMap.get(varName);
            List<Pair<IndependentPredicate, ColumnInfo>> pairs = new ArrayList<>(ipStringList.size());
            for (String ipString : ipStringList){
                IndependentPredicate ip = new IndependentPredicate(ipString);
                pairs.add(new Pair<>(ip, schema.getColumnInfo(ip.getAttributeName())));
            }
            ipMap.put(varName, pairs);
        }
        return ipMap;
    }

    private List<byte[]> downloadRangeAndFilter(String bucketName, String key, long start, long end, int pageSize, int threadId) {
        long rangeLength = end - start + 1;
        String range = "bytes=" + start + "-" + end;

        //System.out.printf("Thread %d starting. Range: %s. Length: %d (Is multiple of %d? %b)%n", threadId, range, rangeLength, pageSize, rangeLength % pageSize == 0);

        GetObjectRequest rangeRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .range(range)
                .build();

        int dataLen = schema.getFixedRecordLen();
        List<byte[]> filteredRecords = new ArrayList<>(4096);

        try (ResponseInputStream<GetObjectResponse> inputStream = s3Client.getObject(rangeRequest)) {
            int bufferCap = 2 * 1024 * 1024;
            byte[] buffer = new byte[bufferCap];

            long totalRead = 0;
            boolean isEOF = false;
            while (!isEOF) {
                int bytesInBatch = 0;
                while (bytesInBatch < bufferCap) {
                    int n = inputStream.read(buffer, bytesInBatch, bufferCap - bytesInBatch);
                    if (n == -1) {
                        isEOF = true;
                        break;
                    }
                    bytesInBatch += n;
                }
                if (bytesInBatch == 0) {
                    break;
                }

                int pageNum = bytesInBatch / pageSize;
                int remainder = bytesInBatch % pageSize;
                int singlePageRecordNum = pageSize / dataLen;
                int extraRecords = remainder / dataLen;
                int recordNum = singlePageRecordNum * pageNum + extraRecords;

                ByteBuffer bb = ByteBuffer.wrap(buffer, 0, bytesInBatch);

                for (int i = 0; i < recordNum; i++) {
                    int pagePos = (i / singlePageRecordNum) * pageSize + (i % singlePageRecordNum) * dataLen;

                    for (List<Pair<IndependentPredicate, ColumnInfo>> ips : ipMap.values()) {
                        boolean satisfied = true;
                        for (Pair<IndependentPredicate, ColumnInfo> pair : ips) {
                            IndependentPredicate ip = pair.getKey();
                            ColumnInfo columnInfo = pair.getValue();
                            Object obj = schema.getColumnValue(columnInfo, bb, pagePos);
                            if (!ip.check(obj, columnInfo.getDataType())) {
                                satisfied = false;
                                break;
                            }
                        }
                        if (satisfied) {
                            byte[] record = new byte[dataLen];
                            System.arraycopy(buffer, pagePos, record, 0, dataLen);
                            filteredRecords.add(record);
                            break;
                        }
                    }
                }
                totalRead += bytesInBatch;
            }
            //System.out.printf("Thread %d finished. Range: [%d - %d]. Processed: %d bytes.\n", threadId, start, end, totalRead);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return filteredRecords;
    }


//    public static void main(String[] args) {
//        List<String> ips1 = new ArrayList<>();
//        ips1.add("V1.BEAT = 1024");//30189
//        ips1.add("V1.TYPE = 'ROBBERY'");//1216
//        Map<String, List<String>> ipStringMap = new HashMap<>();
//        ipStringMap.put("V1", ips1);
//
//        S3Reader downloader = new S3Reader(ipStringMap, "CRIMES");
//        List<byte[]> results = downloader.downloadFileParallel("CRIMES_0.store");
//
//        for(int i = 0; i < Math.min(10, results.size()); i++){
//            byte[] record = results.get(i);
//            System.out.println(downloader.schema.getRecordStr(record));
//        }
//    }
}

/*
s3 select documentation:
https://docs.amazonaws.cn/AmazonS3/latest/userguide/using-select.html

 */

