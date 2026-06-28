package rpc;

import compute.Args;
import org.apache.thrift.TException;
import rpc.iface.DataChunk;
import rpc.iface.PushDownRPC;
import store.EventSchema;
import store.FullScan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PullAllDataImpl implements PushDownRPC.Iface{
    public static int MAX_MEMORY_SIZE = 128 * 1024 * 1024; // 128 MB;
    public static int queryId = 0;

    public EventSchema schema;
    public int chunkId;
    public int recordSize;

    private int callNum = 0;
    List<byte[]> loadedRecords = new ArrayList<>();
    int loadedRecordIdx = 0;
    private FullScan scanner;

    @Override
    public DataChunk pullEvents(String tableName, int offset, Map<String, List<String>> ipMap) throws TException {
        // note that ipMap is null
        if(offset == 0){
            System.out.println(queryId + "-th query arrives....");
            queryId++;
            chunkId = 0;
            schema = EventSchema.getEventSchema(tableName);
            recordSize = schema.getFixedRecordLen();

            callNum = 0;
            scanner = new FullScan(tableName);
        }

        if(loadedRecords.isEmpty()){
            loadedRecords = scanner.concurrentScan(callNum++);
            loadedRecordIdx = 0;
            if(loadedRecords.isEmpty()){
                return new DataChunk(chunkId++, ByteBuffer.allocate(0), true);
            }
        }

        // consume loaded records
        int maxRecordNum = Args.MAX_CHUNK_SIZE / recordSize;
        if(loadedRecords.size() - loadedRecordIdx <= maxRecordNum) {
            // all loaded records can be sent out
            int recordNum = loadedRecords.size() - loadedRecordIdx;
            ByteBuffer buffer = ByteBuffer.allocate(recordNum * recordSize);
            for (int i = 0; i < recordNum; i++) {
                buffer.put(loadedRecords.get(loadedRecordIdx++));
            }
            buffer.flip();
            boolean isLastChunk = false;
            loadedRecords = new ArrayList<>(); // set loadedRecords to empty
            return new DataChunk(chunkId++, buffer, isLastChunk);
        }else{
            // only part of loaded records can be sent out
            ByteBuffer buffer = ByteBuffer.allocate(maxRecordNum * recordSize);
            for (int i = 0; i < maxRecordNum; i++) {
                buffer.put(loadedRecords.get(loadedRecordIdx++));
            }
            buffer.flip();
            boolean isLastChunk = false;
            return new DataChunk(chunkId++, buffer, isLastChunk);
        }
    }
}
