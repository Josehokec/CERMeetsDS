package rpc;

import compute.Args;
import org.apache.thrift.TException;
import rpc.iface.DataChunk;
import rpc.iface.PushDownRPC;
import store.FullScan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PushDownRPCImpl implements PushDownRPC.Iface {
    public static int queryId = 0;
    public List<byte[]> filteredRecords = new ArrayList<>();
    public int chunkId;
    public int recordSize;
    public int MAX_RECORD_NUM;

    @Override
    public DataChunk pullEvents(String tableName, int offset, Map<String, List<String>> ipMap) throws TException {
        // System.out.println("offset: " + offset);

        // initialization
        if(offset == 0){
            System.out.println(queryId + "-th query arrives....");
            queryId++;
            chunkId = 0;

            long startRead = System.currentTimeMillis();
            FullScan fullscan = new FullScan(tableName);
            filteredRecords = fullscan.concurrentScan(ipMap);
            long endRead = System.currentTimeMillis();
            System.out.println("read cost: " + (endRead - startRead) + "ms");

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
}
