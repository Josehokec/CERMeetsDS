package plan;

import parser.QueryParse;
import store.EventSchema;

import java.nio.file.Paths;
import java.util.Map;

public class AdaptiveCostModel implements CostEstimator {
    public int M;                                       // number of storage nodes
    public int l_t = 20;                                // length of slice tag
    public long window;                                 // window size
    public EventSchema schema;
    public Map<String, Double> lambdaMap;               // varName, arrival rate
    Map<String, Integer> varEventNumMap;
    public int dataLen;

    public double beta;
    public double sumLambda;

    public static long SUM_LEN = 50_000_000_000L;       // determined by distribution
    public String samplingFile = Paths.get("src/main/dataset/synthetic50M_sampling.csv").toAbsolutePath().toString();

    public SWFCostPredicateModel swfCostPredicateModel = SWFCostPredicateModel.loadFromFile(Paths.get("src/main/java/plan/SWFCost.txt"));
    public WJFCostPredictModel wjfCostPredictModel = WJFCostPredictModel.loadFromFile(Paths.get("src/main/java/plan/WJFCost.txt"));

    public AdaptiveCostModel(int M, QueryParse query, Map<String, Integer> varEventNumMap, EventSchema schema){
        this.M = M;
        this.schema = schema;
        this.dataLen = schema.getFixedRecordLen();
        this.window = query.getWindow();
        this.varEventNumMap = varEventNumMap;

        int varNum = varEventNumMap.size();
        lambdaMap = new java.util.HashMap<>(varNum << 1);

        for(Map.Entry<String, Integer> eventNumEntry : varEventNumMap.entrySet()){
            String varName = eventNumEntry.getKey();
            int eventNum = eventNumEntry.getValue();
            double lambda = eventNum * 1.0 / SUM_LEN;
            lambdaMap.put(varName, lambda);
            sumLambda += lambda;
        }

        MatchCostPredicateModel model = new MatchCostPredicateModel(samplingFile, schema);
        this.beta = model.getMatchCost(query, schema);
    }

    @Override
    public boolean newRoundTrip(long bitCount, int headOrTail, String varName, int swfSize, int bfSize, int H, int keyLen, int keyNum, double networkBandwidth){
        // if benefit >= 0, we can start a new round trip
        double lambda = lambdaMap.get(varName);
        double currentRISLen = bitCount * 1.0 * window / l_t;
        int bucketNum = swfSize / 16;   // each bucket has 16 bytes <== (12 bits + 20 bits) * 4

        double reducedRatio;
        if(H == 0){
            reducedRatio = headOrTail == 2 ? Math.exp(-2 * lambda * window) : Math.exp(-lambda * window);
        }else{
            reducedRatio = headOrTail == 2 ? Math.exp(-2 * 0.2 * lambda * window) : Math.exp(-0.2 *lambda * window);
        }

        double reducedEvents = reducedRatio * currentRISLen * sumLambda;
        double good = reducedEvents * dataLen / M  / networkBandwidth + beta * reducedEvents;

        double c_p1 = swfCostPredicateModel.getAvgQueryLatency(bucketNum);
        //int idx = headOrTail == 2 ? 2 : 1;
        //double c_update = swfCostPredicateModel.getSWFCost(bucketNum)[idx];
        double bad;
        if(H == 0){
            bad = (lambda * SUM_LEN * c_p1) / M + 2 * (swfSize + bfSize) / networkBandwidth;
            //bad = (lambda * SUM_LEN * c_p1) / M +  c_update * currentRISLen * (1 - reducedRatio) * lambda + 2 * (swfSize + bfSize) / networkBandwidth;
        }else{
            WJFCostPredictModel.WJFCost wjfCost = wjfCostPredictModel.predict(keyLen, keyNum / H); //average key num
            double c_p2 = wjfCost.verificationNs;
            double c_b = wjfCost.buildNs;
            bad = (lambda * SUM_LEN * c_p1 + currentRISLen * lambda * c_p2 * H + keyNum * c_b) / M + 2 * (swfSize + bfSize) / networkBandwidth;
        }

        System.out.println("new round trip networkBandwidth:" + networkBandwidth + "B/ns" + ", " +
                "reduced network cost:" + (long)(reducedEvents * dataLen / M  / networkBandwidth ) + "ns, " +
                "reduced compute cost:" + (long)(beta * reducedEvents) + "ns");
        System.out.println("varName: " + varName + "# estimate good:" + (long)good + "ns, " +
                "bad:" + (long)bad + "ns, benefit: " + (long)(good - bad)+ "ns, " +
                "reducedEvents:" + (int)reducedEvents + ", reducedRatio:" + reducedRatio + ", lambda:" + lambda);

        return good >= bad;
    }
}
