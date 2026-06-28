package utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class ReplayIntervalsTest {

    @Test
    public void insert() {
        int testRangeNum = 1_00_000;
        long window = 10;
        // 10_000_000
        int max = 1_000_000;
        for(int loop = 0; loop < 5; loop++){
            ReplayIntervals intervals = new ReplayIntervals(testRangeNum);
            Random rand = new Random(7);

            long startTime = System.currentTimeMillis();
            for(int i = 0; i < testRangeNum; i++) {
                int start = rand.nextInt(max);
                //intervals.insert(start, start + window);
                intervals.insertInOrder(start, start + window);
            }

            intervals.sortAndReconstruct();
            long endTime = System.currentTimeMillis();
            System.out.println("sort cost: " + (endTime - startTime) + "ms");
        }
    }

    @Test
    public void checkEqual() {
        int testRangeNum = 1_20_000;
        long window = 10;
        // 10_000_000
        int max = 1_000_000;

        ReplayIntervals fastIntervals = new ReplayIntervals(testRangeNum);
        ReplayIntervals slowIntervals = new ReplayIntervals();
        Random rand = new Random(7);

        for(int i = 0; i < testRangeNum; i++) {
            int start = rand.nextInt(max);
            fastIntervals.insert(start, start + window);
            slowIntervals.insertInOrder(start, start + window);
        }

        fastIntervals.sortAndReconstruct();

        Assert.assertTrue(fastIntervals.equals(slowIntervals));
    }

    @Test
    public void compressedRoundTrip() {
        ReplayIntervals intervals = new ReplayIntervals();
        intervals.insert(10, 20);
        intervals.insert(40, 55);
        intervals.insert(100, 130);

        ReplayIntervals decoded = ReplayIntervals.deserializeCompressed(intervals.serializeCompressed());

        Assert.assertTrue(intervals.equals(decoded));
        Assert.assertEquals(intervals.getTimeLength(), decoded.getTimeLength());
    }
}
