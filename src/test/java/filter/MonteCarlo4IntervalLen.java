package filter;

import org.junit.Assert;
import org.junit.Test;
import utils.ExponentialGenerator;
import utils.ReplayIntervals;

import java.util.List;

public class MonteCarlo4IntervalLen {



    public static double getExceptedIntervalLen(int headOrTail, long T, double lambda, long window){
        // head: 0, tail: 1, other: 2

        double cet = Math.exp(-lambda * window);
        if(headOrTail == 0 || headOrTail == 1){
            return T*(1-cet);
            // return window + 1/lambda * cet - 1/lambda + (T- window) * (1 - cet);
        }else{
            return T*(1-cet*cet);
            //return 2*window + 2/lambda * cet * (cet - 1) + (T - 2 * window) * (1 - cet * cet);
        }

    }

    public static long[] getLeftRightOffset(int headOrTail, long window){
        long leftOffset, rightOffset;
        if(headOrTail == 0){
            leftOffset = 0;
            rightOffset = window * 1000;
        }else if(headOrTail == 1){
            leftOffset = -window * 1000;
            rightOffset = 0;
        }else{
            leftOffset = -window * 1000;
            rightOffset = window * 1000;
        }
        return new long[]{leftOffset, rightOffset};
    }


    public static void main(String[] args) {
        int headOrTail_i = 0;
        int headOrTail_j = 1;
        int headOrTail_k = 2;

        long window = 100;

        // i: [0.001, 0.005];
        for(double lambda_i = 0.001; lambda_i <= 0.01; lambda_i += 0.0001){
            for(double lambda_j = lambda_i + 0.002; lambda_j <= 0.015; lambda_j += 0.001){
                for(double lambda_k = lambda_i + 0.005; lambda_k <= 0.02; lambda_k += 0.001){

                    System.out.println("lambda_i: " + lambda_i + ", lambda_j: " + lambda_j + ", lambda_k: " + lambda_k);
                    int tsNum = 10_000;
                    long[] tsList1 = ExponentialGenerator.generateTimestamps(tsNum, lambda_i);
                    long endTs = tsList1[tsNum - 1] + window * 1000;
                    ReplayIntervals replayIntervals_i = new ReplayIntervals();
                    long[] leftRightOffsets_i = getLeftRightOffset(headOrTail_i, window);
                    for(long ts : tsList1){
                        replayIntervals_i.insert(ts + leftRightOffsets_i[0], ts + leftRightOffsets_i[1]);
                    }
                    long realLen_i = replayIntervals_i.getTimeLength();

                    long duration = (endTs - ExponentialGenerator.startTs) / 1000;

                    long theoreticalLen_i = (long) (getExceptedIntervalLen(headOrTail_i, duration, lambda_i, window) * 1000);

                    double ratio1 = Math.abs((realLen_i + 0.0)/theoreticalLen_i - 1) * 100;
                    System.out.println("realLen_i: " + realLen_i + " theoretical len: " + theoreticalLen_i + " relative ratio: " + String.format("%.3f", ratio1) + "%");

                    List<Long> tsList2 = ExponentialGenerator.generateTimestamps(endTs, lambda_j);
                    ReplayIntervals replayIntervals_j = new ReplayIntervals();
                    long[] leftRightOffsets_j = getLeftRightOffset(headOrTail_j, window);
                    for(long ts : tsList2){
                        replayIntervals_j.insert(ts + leftRightOffsets_j[0], ts + leftRightOffsets_j[1]);
                    }
                    replayIntervals_i.intersect(replayIntervals_j);
                    long realLen_j = replayIntervals_i.getTimeLength();

                    // duration -> realLen_i/1000
                    long theoreticalLen_j = (long) (getExceptedIntervalLen(headOrTail_j, realLen_i/1000, lambda_j, window) * 1000);
                    double ratio2 = Math.abs((realLen_j + 0.0)/theoreticalLen_j - 1) * 100;
                    System.out.println("realLen_j: " + realLen_j + " theoretical len: " + theoreticalLen_j + " relative ratio: " + String.format("%.3f", ratio2) + "%");

                    List<Long> tsList3 = ExponentialGenerator.generateTimestamps(endTs, lambda_k);
                    ReplayIntervals replayIntervals_k = new ReplayIntervals();
                    long[] leftRightOffsets_k = getLeftRightOffset(headOrTail_k, window);
                    for(long ts : tsList3){
                        replayIntervals_k.insert(ts + leftRightOffsets_k[0], ts + leftRightOffsets_k[1]);
                    }
                    replayIntervals_i.intersect(replayIntervals_k);
                    long realLen_k = replayIntervals_i.getTimeLength();

                    // duration -> realLen_i/1000
                    long theoreticalLen_k = (long) (getExceptedIntervalLen(headOrTail_k, realLen_j/1000, lambda_k, window) * 1000);
                    double ratio3 = Math.abs((realLen_k + 0.0)/theoreticalLen_k - 1) * 100;
                    System.out.println("realLen_k: " + realLen_k + " theoretical len: " + theoreticalLen_k + " relative ratio: " + String.format("%.3f", ratio3) + "%");
                }
            }
        }

    }


}
