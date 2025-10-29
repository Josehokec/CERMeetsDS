package store;

import filter.OptimizedSWF;

import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.RandomAccessFile;

// return all records within swf
public class ReaderThread4List3 extends Thread{
    private final File file;
    private final EventSchema schema;
    private final long start;
    private final long end;
    private final int dataLen;


    private final OptimizedSWF swf;
    private final long window;
    List<byte[]> filteredRecords;

    // filteredRecords = ByteBuffer.allocate((int)(end - start));

    public ReaderThread4List3(File file, long start, long end, int dataLen, OptimizedSWF swf, EventSchema schema, long window){
        this.file = file;
        this.start = start;
        this.end = end;
        this.dataLen = dataLen;
        this.swf = swf;
        this.schema = schema;
        this.window = window;
        this.filteredRecords = new ArrayList<>(4096);
    }

    public List<byte[]> getFilteredRecords() {
        return filteredRecords;
    }

    @Override
    public void run(){
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(start);
            byte[] buffer = new byte[EventStore.pageSize];
            long bytesRead = 0;
            while (bytesRead < (end - start)) {
                int read = raf.read(buffer);
                if (read == -1) break;
                bytesRead += read;
                int recordNum = read / dataLen;
                for(int i = 0; i < recordNum; i++){
                    byte[] record = new byte[dataLen];
                    System.arraycopy(buffer, i * dataLen, record, 0, dataLen);
                    long ts = schema.getTimestamp(record);
                    if(swf.query(ts, window)){
                        filteredRecords.add(record);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
