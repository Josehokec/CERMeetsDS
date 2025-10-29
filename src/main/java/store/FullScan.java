package store;


import filter.OptimizedSWF;
import parser.IndependentPredicate;
import rpc.PullAllDataImpl;
import s3.S3Reader;
import s3.S3ReaderPlus;
import utils.Pair;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/*
 * we have added S3 as storage backend
 */
public class FullScan {
    // if you have multiple storage nodes, please modify this variable
    // it will scan the files from event_store
    private final String tableName;
    private final String nodeId;
    private boolean enableS3 = false;

    public static int threadNum = 4;

    public FullScan(String tableName) {
        this.tableName = tableName;
        nodeId="";
        //nodeId = "_0";
        // nodeId = "50M_0";
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

    // [pushdown] single thread
    public List<byte[]> scan(Map<String, List<String>> ipStringMap){
        EventSchema schema = EventSchema.getEventSchema(tableName);
        EventStore store = new EventStore(tableName + nodeId, false);
        List<byte[]> filteredRecords = new ArrayList<>(8192);

        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = parseIpString(ipStringMap, schema);

        long fileSize = store.getFileSize();
        int recordLen = schema.getFixedRecordLen();
        File file = store.getFile();

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(0);
            long bytesRead = 0;
            byte[] buffer = new byte[EventStore.pageSize];
            while (bytesRead < fileSize) {
                int read = raf.read(buffer);
                if (read == -1) break;
                bytesRead += read;
                // start process data
                int curRecordNum = read / recordLen;
                ByteBuffer bb = ByteBuffer.wrap(buffer);
                for(int i = 0; i < curRecordNum; i++){
                    // for a single event, check each variable's independent predicates
                    for(Map.Entry<String, List<Pair<IndependentPredicate, ColumnInfo>>> entry : ipMap.entrySet()) {
                        //IndependentPredicate
                        List<Pair<IndependentPredicate, ColumnInfo>> ips = entry.getValue();
                        boolean satisfied = true;
                        for(Pair<IndependentPredicate, ColumnInfo> pair : ips){
                            IndependentPredicate ip = pair.getKey();
                            ColumnInfo columnInfo = pair.getValue();
                            Object obj = schema.getColumnValue(columnInfo, bb, i * recordLen);
                            if(!ip.check(obj, columnInfo.getDataType())){
                                satisfied = false;
                                break;
                            }
                        }
                        if(satisfied){
                            byte[] record = new byte[recordLen];
                            System.arraycopy(buffer, i * recordLen, record, 0, recordLen);
                            filteredRecords.add(record);
                            // we must break to avoid same events
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return filteredRecords;
    }

    // [pushall]
    public List<byte[]> concurrentScan(int callNum){
        if(PullAllDataImpl.MAX_MEMORY_SIZE % (EventStore.pageSize * threadNum) != 0){
            throw new RuntimeException("MAX_MEMORY_SIZE must be multiple of pageSize * threadNum");
        }

        long startPos = callNum * (long) PullAllDataImpl.MAX_MEMORY_SIZE;
        EventSchema schema = EventSchema.getEventSchema(tableName);
        int dataLen = schema.getFixedRecordLen();
        EventStore store = new EventStore(tableName + nodeId, false);
        long fileSize = store.getFileSize();
        File file = store.getFile();

        if(fileSize <= startPos){
            return new ArrayList<>();
        }else if(fileSize - startPos < PullAllDataImpl.MAX_MEMORY_SIZE){
            // read the rest part
            long readPageNum = (fileSize - startPos) / EventStore.pageSize / threadNum;
            List<ReaderThread4List2> threads = new ArrayList<>(threadNum);
            for(int i = 0; i < threadNum; i++){
                long start = startPos + (long) i * readPageNum * EventStore.pageSize;
                long end = i == (threadNum - 1) ? fileSize : start + readPageNum * EventStore.pageSize;
                ReaderThread4List2 thread = new ReaderThread4List2(file, start, end, dataLen);
                threads.add(thread);
                thread.start();
            }
            for(ReaderThread4List2 thread : threads){
                try{
                    thread.join();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            // here we need to merge results
            List<byte[]> ans = new ArrayList<>((int)((fileSize - startPos) / dataLen));
            for(ReaderThread4List2 thread : threads){
                ByteBuffer bb = thread.getFilteredRecords();
                bb.flip();
                while(bb.remaining() >= dataLen){
                    byte[] record = new byte[dataLen];
                    bb.get(record);
                    ans.add(record);
                }
            }
            return ans;
        }else{
            List<ReaderThread4List2> threads = new ArrayList<>(threadNum);
            for(int i = 0; i < threadNum; i++){
                int offset = PullAllDataImpl.MAX_MEMORY_SIZE / threadNum;
                long start = startPos + (long) i * offset;
                long end = start + offset;
                ReaderThread4List2 thread = new ReaderThread4List2(file, start, end, dataLen);
                threads.add(thread);
                thread.start();
            }
            for(ReaderThread4List2 thread : threads){
                try{
                    thread.join();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            // here we need to merge results
            List<byte[]> ans = new ArrayList<>(PullAllDataImpl.MAX_MEMORY_SIZE / dataLen);
            for(ReaderThread4List2 thread : threads){
                ByteBuffer bb = thread.getFilteredRecords();
                bb.flip();
                while(bb.remaining() >= dataLen){
                    byte[] record = new byte[dataLen];
                    bb.get(record);
                    ans.add(record);
                }
            }
            return ans;
        }
    }

    // [pushdown] if storage node has multiple cpus, we can use concurrentScan function
    public List<byte[]> concurrentScan(Map<String, List<String>> ipStringMap){
        if(enableS3){
            S3Reader downloader = new S3Reader(ipStringMap, tableName);
            return downloader.downloadFileParallel(tableName + nodeId + ".store");
        }

        EventSchema schema = EventSchema.getEventSchema(tableName);
        EventStore store = new EventStore(tableName + nodeId, false);
        long fileSize = store.getFileSize();
        long readPageNum = fileSize / EventStore.pageSize / threadNum;

        File file = store.getFile();
        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = parseIpString(ipStringMap, schema);

        List<ReaderThread4List> threads = new ArrayList<>(threadNum);
        for(int i = 0; i < threadNum; i++){
            long start = i * readPageNum * EventStore.pageSize;
            long end = i == (threadNum - 1) ? fileSize : (i + 1) * readPageNum * EventStore.pageSize;
            ReaderThread4List thread = new ReaderThread4List(file, start, end, schema, ipMap);
            threads.add(thread);
            thread.start();
        }

        for(ReaderThread4List thread : threads){
            try{
                thread.join();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        List<byte[]> ans = null;
        for(ReaderThread4List thread : threads){
            if(ans == null){
                ans = thread.getFilteredRecords();
            }else{
                ans.addAll(thread.getFilteredRecords());
            }
        }
        return ans;
    }

    // [pull range]
    public List<byte[]> concurrentScan(OptimizedSWF swf, long window){
        EventSchema schema = EventSchema.getEventSchema(tableName);
        EventStore store = new EventStore(tableName + nodeId, false);
        int dataLen = schema.getFixedRecordLen();
        long fileSize = store.getFileSize();
        long readPageNum = fileSize / EventStore.pageSize / threadNum;

        File file = store.getFile();

        List<ReaderThread4List3> threads = new ArrayList<>(threadNum);
        for(int i = 0; i < threadNum; i++){
            long start = i * readPageNum * EventStore.pageSize;
            long end = i == (threadNum - 1) ? fileSize : (i + 1) * readPageNum * EventStore.pageSize;
            ReaderThread4List3 thread = new ReaderThread4List3(file, start, end, dataLen, swf, schema, window);
            threads.add(thread);
            thread.start();
        }

        for(ReaderThread4List3 thread : threads){
            try{
                thread.join();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        List<byte[]> ans = null;
        for(ReaderThread4List3 thread : threads){
            if(ans == null){
                ans = thread.getFilteredRecords();
            }else{
                ans.addAll(thread.getFilteredRecords());
            }
        }
        return ans;
    }


    // here we will read all events in batch
    // this function use event store to scan, it has a low query speed, so we discard this function
    // please note that we do not require each variable's result cannot overlap
    @Deprecated
    public EventCache scanBasedVarNameInBatch(Map<String, List<String>> ipStringMap){
        List<byte[]> filteredRecords = new ArrayList<>(1024);
        Map<String, List<Integer>> varPointers = new HashMap<>(ipStringMap.size() << 2);
        Map<String, List<IndependentPredicate>> ipMap = new HashMap<>(ipStringMap.size() << 2);

        for(String varName : ipStringMap.keySet()){
            varPointers.put(varName, new ArrayList<>(128));
            List<String> ipStringList = ipStringMap.get(varName);
            List<IndependentPredicate> ips = new ArrayList<>(ipStringList.size());
            for (String ipString : ipStringList){
                IndependentPredicate ip = new IndependentPredicate(ipString);
                ips.add(ip);
            }
            ipMap.put(varName, ips);
        }
        int recordNum = 0;

        EventSchema schema = EventSchema.getEventSchema(tableName);
        EventStore store = new EventStore(tableName + nodeId, false);
        long fileSize = store.getFileSize();

        long hasReadSize = 0;
        int curPage = 0;
        int curOffset = 0;
        int recordLen = schema.getFixedRecordLen();

        // read record in batch
        MappedByteBuffer pageBuffer = store.getMappedBuffer(curPage, fileSize);

        // due to we use fixed length record, we can obtain all record automatically
        while(hasReadSize < fileSize){
            if (curOffset + recordLen > EventStore.pageSize) {
                curPage++;
                hasReadSize += (EventStore.pageSize - curOffset);
                curOffset = 0;
                pageBuffer = store.getMappedBuffer(curPage, fileSize);
            }

            boolean noAdd = true;
            // check independent constraint list, note that ips can be null
            for(Map.Entry<String, List<IndependentPredicate>> entry : ipMap.entrySet()){
                String varName = entry.getKey();
                //IndependentPredicate
                List<IndependentPredicate> ips = entry.getValue();
                boolean satisfied = true;
                for(IndependentPredicate ip : ips){
                    String columnName = ip.getAttributeName();
                    Object obj = schema.getColumnValue(columnName, pageBuffer, curOffset);
                    if(!ip.check(obj, schema.getDataType(columnName))){
                        satisfied = false;
                        break;
                    }
                }

                if(satisfied){
                    if(noAdd){
                        byte[] record = new byte[recordLen];
                        pageBuffer.position(curOffset);
                        pageBuffer.get(record);
                        filteredRecords.add(record);
                        noAdd = false;
                        varPointers.get(varName).add(recordNum);
                    }else{
                        varPointers.get(varName).add(recordNum);
                    }
                }

            }
            if(!noAdd){
                recordNum++;
            }
            hasReadSize += recordLen;
            curOffset += recordLen;
        }

        //for(byte[] record : filteredRecords){
        //    System.out.println(schema.getRecordStr(record));
        //}
        //System.out.println("record number " + recordNum);

        return new EventCache(schema, filteredRecords, varPointers);
    }

    // please note that we require each variable's result cannot overlap
    public EventCache scanBasedVarName(Map<String, List<String>> ipStringMap){
        EventSchema schema = EventSchema.getEventSchema(tableName);
        EventStore store = new EventStore(tableName + nodeId, false);

        List<byte[]> filteredRecords = new ArrayList<>(1024);
        Map<String, List<Integer>> varPointers = new HashMap<>(ipStringMap.size() << 2);

        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = parseIpString(ipStringMap, schema);

        int recordNum = 0;
        long fileSize = store.getFileSize();
        int recordLen = schema.getFixedRecordLen();
        File file = store.getFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(0);
            long bytesRead = 0;
            byte[] buffer = new byte[EventStore.pageSize];
            while (bytesRead < fileSize) {
                int read = raf.read(buffer);
                if (read == -1) break;

                // here we will allow overlap

                bytesRead += read;
                // start process data
                int curRecordNum = read / recordLen;
                ByteBuffer bb = ByteBuffer.wrap(buffer);
                for(int i = 0; i < curRecordNum; i++){
                    for(Map.Entry<String, List<Pair<IndependentPredicate, ColumnInfo>>> entry : ipMap.entrySet()) {
                        String varName = entry.getKey();
                        List<Pair<IndependentPredicate, ColumnInfo>> ips = entry.getValue();
                        boolean satisfied = true;
                        for(Pair<IndependentPredicate, ColumnInfo> pair : ips){
                            IndependentPredicate ip = pair.getKey();
                            ColumnInfo columnInfo = pair.getValue();
                            Object obj = schema.getColumnValue(columnInfo, bb, i * recordLen);
                            //Object obj = schema.getColumnValue(columnName, bb, i * recordLen);
                            if(!ip.check(obj, columnInfo.getDataType())){
                                satisfied = false;
                                break;
                            }
                        }
                        if(satisfied){
                            byte[] record = new byte[recordLen];
                            System.arraycopy(buffer, i * recordLen, record, 0, recordLen);
                            filteredRecords.add(record);
                            varPointers.computeIfAbsent(varName, k -> new ArrayList<>()).add(recordNum);
                            recordNum++;
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new EventCache(schema, filteredRecords, varPointers);
    }

    // concurrent read data
    public EventCache concurrentScanBasedVarName(Map<String, List<String>> ipStringMap){
        if(enableS3){
            S3ReaderPlus downloader = new S3ReaderPlus(ipStringMap, tableName);
            return downloader.downloadFileParallel(tableName + nodeId + ".store");
        }

        EventSchema schema = EventSchema.getEventSchema(tableName);
        EventStore store = new EventStore(tableName + nodeId, false);
        long fileSize = store.getFileSize();
        long readPageNum = fileSize / EventStore.pageSize / threadNum;

        File file = store.getFile();
        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = parseIpString(ipStringMap, schema);
        List<ReaderThread4Map> threads = new ArrayList<>(threadNum);
        for(int i = 0; i < threadNum; i++){
            long start = i * readPageNum * EventStore.pageSize;
            long end = i == (threadNum - 1) ? fileSize : (i + 1) * readPageNum * EventStore.pageSize;
            ReaderThread4Map thread = new ReaderThread4Map(file, start, end, schema, ipMap);
            threads.add(thread);
            thread.start();
        }

        for(ReaderThread4Map thread : threads){
            try{
                thread.join();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        //long startTime = System.currentTimeMillis();
        int estimatedSize = threads.get(threadNum - 1).getFilteredRecords().size() * threadNum;
        List<byte[]> filteredRecords = new ArrayList<>(estimatedSize);
        Map<String, List<Integer>> mergedVarPointers = new HashMap<>(ipStringMap.size() << 2);
        for(String varName : ipMap.keySet()){
            int len = threads.get(threadNum - 1).getVarPointers().get(varName).size() * threadNum;
            mergedVarPointers.put(varName, new ArrayList<>(len));
        }


        for(ReaderThread4Map thread : threads){
            int offset = filteredRecords.size();
            filteredRecords.addAll(thread.getFilteredRecords());
            Map<String, List<Integer>> varPointers = thread.getVarPointers();
            for(Map.Entry<String, List<Integer>> entry : varPointers.entrySet()){
                String varName = entry.getKey();
                List<Integer> pointers = entry.getValue();
                int size = pointers.size();
                for(int i = 0; i < size; i++){
                    pointers.set(i, pointers.get(i) + offset);
                }
                mergedVarPointers.get(varName).addAll(pointers);
            }
        }
        //long endTime = System.currentTimeMillis();
        //System.out.println("merge cost: " + (endTime - startTime) + "ms");
        return new EventCache(schema, filteredRecords, mergedVarPointers);
        //return null;
    }
}
