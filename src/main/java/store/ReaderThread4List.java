package store;

import parser.IndependentPredicate;
import utils.Pair;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// ReaderThead1 returns byte list
public class ReaderThread4List extends Thread{
    private final File file;
    private final long start;
    private final long end;
    private final EventSchema schema;
    private final Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap;
    // query results
    List<byte[]> filteredRecords;


    public ReaderThread4List(File file, long start, long end, EventSchema schema, Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap){
        this.file = file;
        this.start = start;
        this.end = end;
        this.schema = schema;
        this.ipMap = ipMap;
        filteredRecords = new ArrayList<>(4096);
    }

    public List<byte[]> getFilteredRecords(){
        return filteredRecords;
    }

    @Override
    public void run(){
        int dataLen = schema.getFixedRecordLen();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(start);
            byte[] buffer = new byte[EventStore.pageSize];
            long bytesRead = 0;
            while (bytesRead < (end - start)) {
                int read = raf.read(buffer);
                if (read == -1) break;

                bytesRead += read;
                int recordNum = read / dataLen;
                ByteBuffer bb = ByteBuffer.wrap(buffer);
                // please note that we require each variable's result cannot overlap
                for(int i = 0; i < recordNum; i++){
                    for(List<Pair<IndependentPredicate, ColumnInfo>> ips : ipMap.values()) {
                        boolean satisfied = true;
                        for(Pair<IndependentPredicate, ColumnInfo> pair : ips){
                            IndependentPredicate ip = pair.getKey();
                            ColumnInfo columnInfo = pair.getValue();
                            Object obj = schema.getColumnValue(columnInfo, bb, i * dataLen);
                            if(!ip.check(obj, columnInfo.getDataType())){
                                satisfied = false;
                                break;
                            }
                        }
                        if(satisfied){
                            byte[] record = new byte[dataLen];
                            System.arraycopy(buffer, i * dataLen, record, 0, dataLen);
                            filteredRecords.add(record);
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
