package plan;

import parser.QueryParse;
import store.EventSchema;

import java.util.Map;

public class NaiveCostModel implements CostEstimator {
    public int M;                                       // number of storage nodes
    public long window;                                 // window size
    public EventSchema schema;
    public Map<String, Double> lambdaMap;               // varName, arrival rate
    Map<String, Integer> varEventNumMap;
    public int dataLen;

    public double sumLambda;
    public long SUM_LEN = 50_000_000_000L;              // determined by distribution

    public NaiveCostModel(int M, QueryParse query, Map<String, Integer> varEventNumMap, EventSchema schema){
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
    }

    @Override
    public boolean newRoundTrip(long bitCount, int headOrTail, String varName, int swfSize, int bfSize, int H, int keyLen, int keyNum, double networkBandwidth) {
        double lambda = lambdaMap.get(varName);
        if(H > 0){
            lambda /= 5;
        }
        double reducedRatio = headOrTail == 2 ? Math.exp(-2 * lambda * window) : Math.exp(-lambda * window);
        return reducedRatio > 0.15;
    }
}
