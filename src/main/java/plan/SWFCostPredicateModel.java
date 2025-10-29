package plan;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SWFCostPredicateModel {
    public HashMap<Integer, Double[]> swfCostMap;
    public SWFCostPredicateModel(HashMap<Integer, Double[]> swfCostMap){
        this.swfCostMap = swfCostMap;
    }

    public static SWFCostPredicateModel loadFromFile(Path path) {
        HashMap<Integer, Double[]> swfCostMap = new HashMap<>();
        try{
            List<String> lines = Files.readAllLines(path);
            Pattern p = Pattern.compile("bucketNum:\\s*(\\d+).*?avgQueryLatency.*?:\\s*([0-9.]+).*?updateLatency1.*?:\\s*([0-9.]+).*?updateLatency2.*?:\\s*([0-9.]+)", Pattern.CASE_INSENSITIVE);
            for (String line : lines) {
                Matcher m = p.matcher(line);
                if (m.find()) {
                    try {
                        int bucketNum = Integer.parseInt(m.group(1));
                        double q = Double.parseDouble(m.group(2));
                        double u1 = Double.parseDouble(m.group(3));
                        double u2 = Double.parseDouble(m.group(4));
                        swfCostMap.put(bucketNum, new Double[]{q, u1, u2});
                    } catch (NumberFormatException ignored) {
                        System.out.println("Skipping invalid line: " + line);
                    }
                }
            }

        }catch (IOException e) {
            e.printStackTrace();
        }
        return new SWFCostPredicateModel(swfCostMap);
    }

    public Double[] getSWFCost(int bucketNum) {
        if(swfCostMap.containsKey(bucketNum)) {
            return swfCostMap.get(bucketNum);
        }else{
            Set<Integer> keys = swfCostMap.keySet();
            int minDiff = Integer.MAX_VALUE;
            int targetKey = -1;
            for(Integer key : keys){
                int diff = Math.abs(key - bucketNum);
                if(diff < minDiff){
                    minDiff = diff;
                    targetKey = key;
                }
            }
            return swfCostMap.get(targetKey);
        }
    }

    public double getAvgQueryLatency(int bucketNum) {
        Double[] costs = swfCostMap.get(bucketNum);
        if(costs != null){
            return costs[0];
        }else{
            Set<Integer> keys = swfCostMap.keySet();
            int minDiff = Integer.MAX_VALUE;
            int targetKey = -1;
            for(Integer key : keys){
                int diff = Math.abs(key - bucketNum);
                if(diff < minDiff){
                    minDiff = diff;
                    targetKey = key;
                }
            }
            return swfCostMap.get(targetKey)[0];
        }
    }

    public static void main(String[] args) throws IOException {
        SWFCostPredicateModel model = SWFCostPredicateModel.loadFromFile(Paths.get("src/main/java/plan/SWFCost.txt"));

        int queryBucket = 8388608;
        Double[] costs = model.getSWFCost(queryBucket);
        System.out.println("Bucket: " + queryBucket +
                ", Avg Query Latency: " + costs[0] +
                ", Update Latency 1: " + costs[1] +
                ", Update Latency 2: " + costs[2]);
    }
}