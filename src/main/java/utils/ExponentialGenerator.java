package utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ExponentialGenerator {
    static final Random random = new Random(17);
    // 2025-03-28 15:23:26 [ms]
    public static final long startTs = 1743146606000L;

    public static double nextExponential(double lambda) {
        double u = random.nextDouble(); // Uniformly distributed random number in [0, 1)
        return -Math.log(1 - u) / lambda;
    }

    /**
     * Randomly select n elements uniformly from a given weighted list
     * (without replacement sampling).
     * @param items     completed string list
     * @param weights   weight list
     * @param n         number of string to be selected
     * @return          string list with length n
     * @throws IllegalArgumentException If n is greater than the size of the list or if the weights do not match
     */
    public static List<String> selectNWeightedStrings(List<String> items, List<Double> weights, int n) {
        if (items.size() != weights.size()) {
            throw new IllegalArgumentException("items and weights must have the same size.");
        }
        if (n > items.size()) {
            throw new IllegalArgumentException("n must be less than items size");
        }

        // create an indices
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            indices.add(i);
        }

        List<String> selected = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < n; i++) {
            double totalWeight = 0.0;
            for (Integer index : indices) {
                totalWeight += weights.get(index);
            }

            double r = random.nextDouble() * totalWeight;
            double cumulative = 0.0;
            int selectedIndex = -1;
            for (Integer index : indices) {
                cumulative += weights.get(index);
                if (r <= cumulative) {
                    selectedIndex = index;
                    break;
                }
            }

            if (selectedIndex == -1) {
                selectedIndex = indices.get(indices.size() - 1);
            }

            selected.add(items.get(selectedIndex));
            indices.remove(Integer.valueOf(selectedIndex));
        }

        return selected;
    }


    public static List<Long> generateTimestamps(long endTs, double lambda){
        long previousTs = startTs;

        List<Long> ans = new ArrayList<>(1024 * 16);

        while(previousTs < endTs){
            double distance = ExponentialGenerator.nextExponential(lambda);
            long curDistance = (long) (distance * 1000);// ms
            previousTs += curDistance;
            ans.add(previousTs);
        }

        return ans;
    }

    public static long[] generateTimestamps(int n, double lambda) {
        long previousTs = startTs;
        //double sum = 0;
        long[] timestamps = new long[n];
        for(int i = 0; i < n; i++) {
            double distance = ExponentialGenerator.nextExponential(lambda);
            //sum += distance;
            long curDistance = (long) (distance * 1000);// ms
            previousTs += curDistance;
            timestamps[i] = previousTs;
        }
        //System.out.println("sum = " + (long) (sum));
        return timestamps;
    }

    public static List<Pair<Long, String>> generateAttributeWindowPair(int n, double lambda){
        long previousTs = startTs;
        long[] timestamps = new long[n];
        List<Pair<Long, String>> attributeWindowPairs = new ArrayList<>(n);
        for(int i = 0; i < n; i++) {
            double distance = ExponentialGenerator.nextExponential(lambda);
            //sum += distance;
            long curDistance = (long) (distance * 1000);
            previousTs += curDistance;
            timestamps[i] = previousTs;
        }

        List<String> list = Arrays.asList(
                "Key1", "Key2", "Key3", "Key4", "Key5", "Key6", "Key7", "Key8", "Key9", "Key10",
                "Key11", "Key12", "Key13", "Key14", "Key15", "Key16", "Key17", "Key18", "Key19", "Key20"
        );

        List<Double> weights = Arrays.asList(
                0.025, 0.0375, 0.05, 0.0625, 0.075,
                0.025, 0.0375, 0.05, 0.0625, 0.075,
                0.025, 0.0375, 0.05, 0.0625, 0.075,
                0.025, 0.0375, 0.05, 0.0625, 0.075
        );

        int distinctN = 5;

        if(n % distinctN != 0){
            throw new RuntimeException("n % int distinctN != 0");
        }

        for(int i = 0; i < n / distinctN; i++) {
            List<String> selected = selectNWeightedStrings(list, weights, distinctN);
            for(int j = 0; j < selected.size(); j++) {
                attributeWindowPairs.add(new Pair<>(timestamps[i * distinctN + j], selected.get(j)));
            }
        }

        //System.out.println("sum = " + (long) (sum));
        return attributeWindowPairs;
    }

    public static long[] generateShuffledTimestamps(int n, double lambda) {
        long[] timestamps = generateTimestamps( n, lambda);
        // random shuffle
        for (int i = timestamps.length - 1; i > 0; i--) {
            int index = random.nextInt(30);
            index = Math.max(0, i - index);
            long a = timestamps[index];
            timestamps[index] = timestamps[i];
            timestamps[i] = a;
        }
        return timestamps;
    }
}
