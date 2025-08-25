package utils;

import store.EventSchema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class SortByTs {
    public static List<byte[]> sort(List<byte[]> records, EventSchema schema){
        int size = records.size();
        List<Pair<Long, byte[]>> recordWithTsList = new ArrayList<>(size);
        for(byte[] record : records){
            recordWithTsList.add(new Pair<>(schema.getTimestamp(record), record));
        }
        recordWithTsList.sort(Comparator.comparingLong(Pair::getKey));
        List<byte[]> ans = new ArrayList<>(size);
        for(Pair<Long, byte[]> recordWithTs : recordWithTsList){
            ans.add(recordWithTs.getValue());
        }
        return ans;
    }

    public static List<byte[]> sortByStream(List<byte[]> records, EventSchema schema){
        int size = records.size();
        List<Pair<Long, byte[]>> recordWithTsList = new ArrayList<>(size);
        for(byte[] record : records){
            recordWithTsList.add(new Pair<>(schema.getTimestamp(record), record));
        }


        recordWithTsList.parallelStream()
                .sorted(Comparator.comparingLong(Pair::getKey))
                .collect(Collectors.toList());

        List<byte[]> ans = new ArrayList<>(size);
        for(Pair<Long, byte[]> recordWithTs : recordWithTsList){
            ans.add(recordWithTs.getValue());
        }
        return ans;
    }
}
