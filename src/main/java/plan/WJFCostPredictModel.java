package plan;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WJFCostPredictModel {
    public static class WJFCost {
        public final double verificationNs;
        public final double buildNs;
        public WJFCost(double v, double b) { verificationNs = v; buildNs = b; }
        public String toString() { return String.format("ver=%.6f ns, build=%.6f ns", verificationNs, buildNs); }
    }

    // data: keyLength -> (keyNum -> WJFCost)
    private final TreeMap<Integer, TreeMap<Long, WJFCost>> data = new TreeMap<>();

    private static final Pattern LINE = Pattern.compile(
            "key length:\\s*(\\d+),\\s*key num:\\s*(\\d+),\\s*verification cost \\(ns\\):\\s*([0-9.]+),\\s*build cost \\(ns\\):\\s*([0-9.]+)"
    );

    public static WJFCostPredictModel loadFromFile(Path path) {
        WJFCostPredictModel model = new WJFCostPredictModel();
        try{
            List<String> lines = Files.readAllLines(path);
            for (String ln : lines) {
                Matcher m = LINE.matcher(ln);
                if (!m.find()) continue;
                int keyLen = Integer.parseInt(m.group(1));
                long keyNum = Long.parseLong(m.group(2));
                double ver = Double.parseDouble(m.group(3));
                double build = Double.parseDouble(m.group(4));
                model.data.computeIfAbsent(keyLen, k -> new TreeMap<>())
                        .put(keyNum, new WJFCost(ver, build));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return model;
    }

    // predict WJFCost for arbitrary keyLen and keyNum (keyNum interpolation uses log2 space)
    public WJFCost predict(int keyLen, long keyNum) {
        if (data.isEmpty()) throw new IllegalStateException("no data loaded");

        // find surrounding key lengths
        Integer kLower = data.floorKey(keyLen);
        Integer kUpper = data.ceilingKey(keyLen);
        if (kLower == null) kLower = data.firstKey();
        if (kUpper == null) kUpper = data.lastKey();

        WJFCost cLower = interpAlongKeyNum(data.get(kLower), keyNum);
        if (kLower.equals(kUpper)) return cLower;

        WJFCost cUpper = interpAlongKeyNum(data.get(kUpper), keyNum);

        // linear interpolation in key length dimension
        double x1 = kLower;
        double x2 = kUpper;
        double t = (x2 == x1) ? 0.0 : ((keyLen - x1) / (x2 - x1));

        double ver = lerp(cLower.verificationNs, cUpper.verificationNs, t);
        double build = lerp(cLower.buildNs, cUpper.buildNs, t);
        return new WJFCost(ver, build);
    }

    // Helper: interpolate within one TreeMap<Long,WJFCost> along keyNum using log2(keyNum)
    private static WJFCost interpAlongKeyNum(TreeMap<Long, WJFCost> map, long keyNum) {
        if (map.containsKey(keyNum)) return map.get(keyNum);

        Long yLower = map.floorKey(keyNum);
        Long yUpper = map.ceilingKey(keyNum);
        if (yLower == null) yLower = map.firstKey();
        if (yUpper == null) yUpper = map.lastKey();

        WJFCost cL = map.get(yLower);
        WJFCost cU = map.get(yUpper);

        if (yLower.equals(yUpper)) return cL;

        double log2Target = Math.log(keyNum) / Math.log(2);
        double log2L = Math.log(yLower) / Math.log(2);
        double log2U = Math.log(yUpper) / Math.log(2);

        double t = (log2U == log2L) ? 0.0 : ((log2Target - log2L) / (log2U - log2L));

        double ver = lerp(cL.verificationNs, cU.verificationNs, t);
        double build = lerp(cL.buildNs, cU.buildNs, t);
        return new WJFCost(ver, build);
    }

    private static double lerp(double a, double b, double t) {
        return a + (b - a) * t;
    }

    public static void main(String[] args) throws Exception {
        Path p = Paths.get("src/main/java/plan/WJFCost.txt");
        WJFCostPredictModel model = WJFCostPredictModel.loadFromFile(p);

        WJFCost exact = model.predict(12, 16777216L);
        System.out.println("predict(12,16777216) => " + exact);

        WJFCost interp = model.predict(10, 500000L);
        System.out.println("predict(10,500000) => " + interp);

        WJFCost interp2 = model.predict(1, 2L);
        System.out.println("predict(1,2L) => " + interp2);
    }
}
