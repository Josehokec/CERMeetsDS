package rpc;

import compute.Args;
import org.apache.thrift.TException;
import rpc.iface.DataChunk;
import rpc.iface.PushDownRPC;
import store.EventStore;
import store.FullScan;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PushDownRPCImpl implements PushDownRPC.Iface {
    private static final int DISABLE_COMPUTE_CACHE_OFFSET = Integer.MIN_VALUE;

    public static int queryId = 0;
    public List<byte[]> filteredRecords = new ArrayList<>();
    public int chunkId;
    public int recordSize;
    public int MAX_RECORD_NUM;
    private File cachePageTransferFile;
    private long cachePageTransferOffset;
    private long cachePageTransferLimit;
    private String computeCacheTableName;
    private int computeCachePageNum;

    @Override
    public synchronized DataChunk pullEvents(String tableName, int offset, Map<String, List<String>> ipMap) throws TException {
        // System.out.println("offset: " + offset);
        if(offset == DISABLE_COMPUTE_CACHE_OFFSET){
            disableComputeCache(tableName);
            return new DataChunk(-1, ByteBuffer.allocate(0), true);
        }
        if(offset < 0){
            return getComputeCachePages(tableName, -(long) offset);
        }

        // initialization
        if(offset == 0){
            System.out.println(queryId + "-th query arrives....");
            queryId++;
            chunkId = 0;

            long startRead = System.currentTimeMillis();
            int skipPageNum = getComputeCachePageNum(tableName);
            if(skipPageNum > 0){
                System.out.println("skip compute-cached leading pages during push-down scan: " + skipPageNum);
            }
            FullScan fullscan = new FullScan(tableName, skipPageNum);
            filteredRecords = fullscan.concurrentScan(ipMap);
            long endRead = System.currentTimeMillis();
            System.out.println("read cost: " + (endRead - startRead) + "ms");

            if (filteredRecords == null || filteredRecords.isEmpty()) {
                return new DataChunk(-1, ByteBuffer.allocate(0), true);
            }

            // note that if filteredRecords is empty, this will occur exception
            recordSize = filteredRecords.get(0).length;
            MAX_RECORD_NUM = Args.MAX_CHUNK_SIZE / recordSize;

            //double spaceSize = filteredRecords.size() * recordSize * 1.0 / 1024 / 1024;
            //System.out.println("Theoretical space overhead: " +  String.format("%.2f", spaceSize) + "MB");
        }

        // we can transmit remaining all events
        int remaining = filteredRecords.size() - offset;
        DataChunk dataChunk;
        if(remaining <= MAX_RECORD_NUM){
            ByteBuffer buffer = ByteBuffer.allocate(remaining * recordSize);
            for(int i = offset; i < filteredRecords.size(); i++){
                buffer.put(filteredRecords.get(i));
            }
            buffer.flip();
            dataChunk = new DataChunk(chunkId, buffer, true);
        }else{
            ByteBuffer buffer = ByteBuffer.allocate(MAX_RECORD_NUM * recordSize);
            for(int i = offset; i < offset + MAX_RECORD_NUM; i++){
                buffer.put(filteredRecords.get(i));
            }
            buffer.flip();
            dataChunk = new DataChunk(chunkId++, buffer, false);
        }
        return dataChunk;
    }

    private DataChunk getComputeCachePages(String tableName, long cachePageNum) throws TException {
        if(cachePageTransferFile == null || !tableName.equals(computeCacheTableName)){
            EventStore store = new EventStore(tableName + System.getProperty("nodeId", "_0"), false);
            cachePageTransferFile = store.getFile();
            cachePageTransferOffset = 0;
            long requestedBytes = cachePageNum > Long.MAX_VALUE / EventStore.pageSize ?
                    Long.MAX_VALUE : cachePageNum * (long) EventStore.pageSize;
            cachePageTransferLimit = Math.min(store.getFileSize(), requestedBytes);
            computeCacheTableName = tableName;
            long cachedPages = (cachePageTransferLimit + EventStore.pageSize - 1) / EventStore.pageSize;
            computeCachePageNum = (int) Math.min(Integer.MAX_VALUE, cachedPages);
            System.out.println("push-down compute cache configured by compute node: tableName=" + tableName
                    + ", requestedPages=" + cachePageNum
                    + ", cachedPages=" + computeCachePageNum
                    + ", cachedBytes=" + cachePageTransferLimit);
        }

        if(cachePageTransferFile == null || cachePageTransferOffset >= cachePageTransferLimit){
            resetCachePageTransfer();
            return new DataChunk(-1, ByteBuffer.allocate(0), true);
        }

        int readSize = (int) Math.min(Args.MAX_CHUNK_SIZE, cachePageTransferLimit - cachePageTransferOffset);
        byte[] bytes = new byte[readSize];
        int readOffset = 0;
        try(RandomAccessFile raf = new RandomAccessFile(cachePageTransferFile, "r")){
            raf.seek(cachePageTransferOffset);
            while(readOffset < readSize){
                int read = raf.read(bytes, readOffset, readSize - readOffset);
                if(read == -1){
                    break;
                }
                readOffset += read;
            }
        }catch (Exception e){
            throw new TException("Failed to transfer push-down compute cache pages from "
                    + cachePageTransferFile.getAbsolutePath(), e);
        }

        cachePageTransferOffset += readOffset;
        boolean isLastChunk = cachePageTransferOffset >= cachePageTransferLimit || readOffset < readSize;
        ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, readOffset);
        if(isLastChunk){
            resetCachePageTransfer();
        }
        return new DataChunk(-1, buffer, isLastChunk);
    }

    private void resetCachePageTransfer(){
        cachePageTransferFile = null;
        cachePageTransferOffset = 0;
        cachePageTransferLimit = 0;
    }

    private void disableComputeCache(String tableName) {
        computeCacheTableName = tableName;
        computeCachePageNum = 0;
        resetCachePageTransfer();
        System.out.println("push-down compute cache disabled by compute node: tableName=" + computeCacheTableName);
    }

    private int getComputeCachePageNum(String tableName) {
        if(computeCacheTableName == null || !computeCacheTableName.equals(tableName)){
            return 0;
        }
        return computeCachePageNum;
    }
}
