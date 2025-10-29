package store;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

// return all records
public class ReaderThread4List2 extends Thread{
    private final File file;
    private final long start;
    private final long end;
    private final int dataLen;
    ByteBuffer filteredRecords;

    // filteredRecords = ByteBuffer.allocate((int)(end - start));

    public ReaderThread4List2(File file, long start, long end, int dataLen){
        this.file = file;
        this.start = start;
        this.end = end;
        this.dataLen = dataLen;

        int remainBytes = (int) ((end - start) % EventStore.pageSize);
        // if remainBytes != 0, then remainBytes % dataLen == 0

        int pageNum = (int) ((end - start) / EventStore.pageSize);
        if(remainBytes == 0){
            filteredRecords = ByteBuffer.allocate(pageNum * (EventStore.pageSize / dataLen) * dataLen);
        }
        else{
            filteredRecords = ByteBuffer.allocate(pageNum * (EventStore.pageSize / dataLen) * dataLen + remainBytes);
        }
    }

    public ByteBuffer getFilteredRecords() {
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
                filteredRecords.put(buffer, 0, recordNum * dataLen);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
