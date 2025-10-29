package plan;


import filter.WindowWiseBF;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * WJFCostTest is a class for testing the cost of WJF (Windowed Join Filter).
 * the cost of WJF depends on the size of bit array and the length of attribute value
 */
public class WJFCostTest {

    public static long startWindowId = 1007;

    public static List<String> colValues = new ArrayList<>();

    public static double[] getVerificationCost(WindowWiseBF bf){
        int checkTimes = 12_000_000;
        int pickNum = 12;
        List<String> keys = pickRandomSubset(colValues, pickNum, 7);
        List<Object> objs = new ArrayList<>(keys.size());
        objs.addAll(keys);

        int windowNum = checkTimes / pickNum;
        int count = 0;
        long startTime = System.nanoTime();
        for(long windowId = startWindowId; windowId < startWindowId + windowNum; windowId++){
            for(Object obj : objs){
                // please note that we need to check twice to simulate the real verification process
                if(bf.contain(obj.toString(), windowId) ||bf.contain(obj.toString(), windowId + 1)){
                    count++;
                }
            }
        }
        long endTime = System.nanoTime();
        double totalTime = endTime - startTime;
        return new double[]{totalTime / checkTimes, (double) count / checkTimes};
    }

    private static final char[] CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

    public static List<String> generateRandomStr(long seed, int L, int m) {
        List<String> result = new ArrayList<>();
        if (L <= 0 || m <= 0) return result;
        Random rnd = new Random(seed);
        StringBuilder sb = new StringBuilder(L);
        for (int i = 0; i < m; i++) {
            sb.setLength(0);
            for (int j = 0; j < L; j++) {
                sb.append(CHARS[rnd.nextInt(CHARS.length)]);
            }
            result.add(sb.toString());
        }
        return result;
    }

    public static List<String> pickRandomSubset(List<String> src, int n, long seed) {
        if (src == null) return Collections.emptyList();
        int m = src.size();
        if (n <= 0) return Collections.emptyList();
        if (n >= m) return new ArrayList<>(src);

        List<String> copy = new ArrayList<>(src);
        Random rnd = new Random(seed);
        for (int i = 0; i < n; i++) {
            int j = i + rnd.nextInt(m - i); // 在 [i, m-1] 中随机选一个
            Collections.swap(copy, i, j);
        }
        return new ArrayList<>(copy.subList(0, n));
    }

    public static void main(String[] args){
        /*
        warm up for keyLen = 1
         */
        // WindowWiseBF(DEFAULT_FPR, keyNum);
        double DEFAULT_FPR = 0.01;//==> 0.02 due to check twice
        int[] keyNums = {256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216};
        int[] keyLengths = {1, 4, 8, 12, 16, 20, 24, 28, 32};
        for(int keyLen : keyLengths){
            System.out.println("=== Key Length: " + keyLen + " ===");
            int strNum = 20;
            int windowPerKey = 8;
            int seed = 7;
            colValues = WJFCostTest.generateRandomStr(7, keyLen, strNum);


            for(int keyNum : keyNums){
                int windowNum = keyNum / windowPerKey;
                // build window-wise join filter
                long startTimeBuild = System.nanoTime();
                WindowWiseBF bf = new WindowWiseBF(DEFAULT_FPR, keyNum);
                long wid = startWindowId;
                Random random = new Random(7);
                for(int j = 0; j < windowNum; j++){
                    List<String> keysInWindow = pickRandomSubset(colValues, windowPerKey, seed);
                    for(String key : keysInWindow){
                        bf.insert(key, wid);
                        wid += random.nextInt(5) + 1;
                    }
                }
                long endTimeBuild = System.nanoTime();
                double buildCost = (endTimeBuild - startTimeBuild) / (double) keyNum;
                // System.out.println("build cost (ns): " + buildCost);
                // int bitArraySize = bf.serialize().capacity();
                double verificationCost = getVerificationCost(bf)[0];
                // ", bit array size (bytes): " + bitArraySize
                System.out.println("key length: " + keyLen + ", key num: " + keyNum + ", verification cost (ns): " + verificationCost + ", build cost (ns): " + buildCost);
            }
        }
    }
}
