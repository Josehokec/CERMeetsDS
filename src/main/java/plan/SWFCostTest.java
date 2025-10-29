package plan;

import filter.OptimizedSWF;
import filter.UpdatedMarkers;
import utils.ReplayIntervals;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SWFCostTest {

    // key: number of distinct keys inserted
    public static Map<Integer, Double> queryLatencyMap = new HashMap<>();

    // new time interval is [ts, ts + w] or [ts - w, ts]
    public static Map<Integer, Double> updateLatency1Map = new HashMap<>();

    // new time interval is [ts - w, ts + w]
    public static Map<Integer, Double> updateLatency2Map = new HashMap<>();

    public static void updateShrinkFilter(long startTs, long endTs, long window, OptimizedSWF swf, UpdatedMarkers updatedMarkers){
        long startWindowId = startTs / window;
        long endWindowId = endTs / window;

        int leftMarker = OptimizedSWF.getLeftIntervalMarker(startTs, startWindowId * window, window);
        int rightMarker = OptimizedSWF.getRightIntervalMarker(endTs, endWindowId * window, window);
        int leftPosition = swf.queryWindowId(startWindowId);
        if(leftPosition != 0x3){
            updatedMarkers.update(leftPosition >>> 2, leftPosition & 0x1, leftMarker);
        }

        int rightPosition = swf.queryWindowId(endWindowId);
        if(rightPosition != 0x3){
            updatedMarkers.update(rightPosition >>> 2, rightPosition & 0x1, rightMarker);
        }
        for(long wid = startWindowId + 1; wid < endWindowId; wid++){
            int position = swf.queryWindowId(wid);
            if(position != 0x3){
                updatedMarkers.update(position >>> 2, position & 0x1, 0xfffff);
            }
        }
    }

    // keyNumber: 2^4, 2^5, 2^6, 2^7, 2^8, 2^9, 2^10, 2^11, ..., 2^28
    public static double[] obtainInsertionLatency(int keyNum){
        long windowSize = 1139;
        long startWindowId = 1307;
        int checkTimes = 4_000_000;
        long initialWindowId = startWindowId;
        Random random = new Random(7);

        ReplayIntervals intervals = new ReplayIntervals(keyNum);

        OptimizedSWF swf = new OptimizedSWF.Builder(keyNum * 2L).build();
        int bucketNum = swf.getBucketNum();

        for(int i = 0; i < keyNum; i++){
            long start = startWindowId * windowSize + random.nextInt(250);
            long end = (startWindowId + 1) * windowSize - random.nextInt(250) - 1;
            intervals.insert(start, end);
            swf.insert(start, end, windowSize);
            if(i != keyNum - 1){
                startWindowId += random.nextInt(3) + 1;
            }
        }

        // point query...
        long windowNum = startWindowId - initialWindowId;
        long pointNum = windowNum * windowSize;

        long queryStart = System.nanoTime();

        if(checkTimes > pointNum){
            int count = 0;
            while(count < checkTimes){
                for(long ts = initialWindowId * windowSize; ts < (initialWindowId + windowNum) * windowSize; ts++){
                    swf.query(ts, windowSize);
                    count++;
                    if(count >= checkTimes){
                        break;
                    }
                }
                //System.out.println("breakpoint count: " + count);
            }
        }else{
            double gap = pointNum / (checkTimes + 0.0);
            long ts0 = initialWindowId * windowSize;
            for(int i = 0; i < checkTimes; i++){
                long checkTs = (long)(i * gap + ts0);
                swf.query(checkTs, windowSize);
            }
        }
        long queryEnd = System.nanoTime();
        double avgQueryLatency = (queryEnd - queryStart + 0.0) / checkTimes;

        // insert new intervals with [ts, ts + w] or [ts - w, ts]
        long updateStart1 = System.nanoTime();
        UpdatedMarkers updatedMarkers1 = new UpdatedMarkers(bucketNum);
        for(int i = 0; i < checkTimes; i++){
            long start = initialWindowId * windowSize + (long) (random.nextDouble() * (windowNum * windowSize));
            long end = start + windowSize;
            intervals.insert(start, end);
            updateShrinkFilter(start, end, windowSize, swf, updatedMarkers1);
        }
        long updateEnd1 = System.nanoTime();
        double updateLatency1 = (updateEnd1 - updateStart1 + 0.0) / checkTimes;

        // insert new intervals with [ts - w, ts + w]
        long updateStart2 = System.nanoTime();
        UpdatedMarkers updatedMarkers2 = new UpdatedMarkers(bucketNum);
        for(int i = 0; i < checkTimes; i++){
            long center = initialWindowId * windowSize + (long) (random.nextDouble() * (windowNum * windowSize));
            long start = center - windowSize;
            long end = center + windowSize;
            intervals.insert(start, end);
            updateShrinkFilter(start, end, windowSize, swf, updatedMarkers2);
        }
        long updateEnd2 = System.nanoTime();
        double updateLatency2 = (updateEnd2 - updateStart2 + 0.0) / checkTimes;

        //return bucketNum, avgQueryLatency, updateLatency1, updateLatency2
        return new double[]{bucketNum, avgQueryLatency, updateLatency1, updateLatency2};
    }

    public static void main(String[] args) throws FileNotFoundException {
        String filePath = "SWFCost.txt";
        System.setOut(new PrintStream(filePath));

        for(int exp = 25; exp <= 28; exp++){
            int keyNum = (1 << exp) * 3 / 4;
            double[] res = obtainInsertionLatency(keyNum);
            int bucketNum = (int) res[0];
            double avgQueryLatency = res[1];
            double updateLatency1 = res[2];
            double updateLatency2 = res[3];

            queryLatencyMap.put(keyNum, avgQueryLatency);
            updateLatency1Map.put(keyNum, updateLatency1);
            updateLatency2Map.put(keyNum, updateLatency2);

            System.out.println("exp: " + exp + ", bucketNum: " + bucketNum +
                    ", avgQueryLatency (ns): " + avgQueryLatency +
                    ", updateLatency1 (ns): " + updateLatency1 +
                    ", updateLatency2 (ns): " + updateLatency2);
        }

    }
}
