package store;

import parser.IndependentPredicate;
import utils.Pair;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// ReadThread2 return part of cache
public class ReaderThread4Map extends Thread{
    private final File file;
    private final long start;
    private final long end;
    private final EventSchema schema;
    private final Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap;

    // query results
    List<byte[]> filteredRecords;
    Map<String, List<Integer>> varPointers;

    public ReaderThread4Map(File file, long start, long end, EventSchema schema, Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap) {
        this.file = file;
        this.start = start;
        this.end = end;
        this.schema = schema;
        this.ipMap = ipMap;
        filteredRecords = new ArrayList<>(4096);
        varPointers = new HashMap<>();
    }

    public List<byte[]> getFilteredRecords() {
        return filteredRecords;
    }

    public Map<String, List<Integer>> getVarPointers() {
        return varPointers;
    }

    @Override
    public void run() {
        //long startTime = System.currentTimeMillis();
        int dataLen = schema.getFixedRecordLen();
        int cnt = 0;

        // new code lines to avoid null pointer
        for(String varName : ipMap.keySet()) {
            varPointers.put(varName, new ArrayList<>(2048));
        }

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
                for(int i = 0; i < recordNum; i++){
                    boolean noInserted = true;
                    for(Map.Entry<String, List<Pair<IndependentPredicate, ColumnInfo>>> entry : ipMap.entrySet()) {
                        List<Pair<IndependentPredicate, ColumnInfo>> ips = entry.getValue();
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
                            if(noInserted){
                                byte[] record = new byte[dataLen];
                                System.arraycopy(buffer, i * dataLen, record, 0, dataLen);
                                filteredRecords.add(record);
                                varPointers.get(entry.getKey()).add(cnt);
                                cnt++;
                                noInserted = false;
                            }else{
                                varPointers.get(entry.getKey()).add(cnt - 1);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //long endTime = System.currentTimeMillis();
        //System.out.println("scan cost " + (endTime - startTime) + "ms");
    }
}
