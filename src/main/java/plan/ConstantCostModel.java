package plan;

import parser.QueryParse;
import store.EventSchema;

import java.nio.file.Paths;
import java.util.*;

public class ConstantCostModel implements CostEstimator {
    public int M;                                       // number of storage nodes
    public long window;                                 // window size
    public EventSchema schema;
    public Map<String, Double> lambdaMap;               // varName, arrival rate
    Map<String, Integer> varEventNumMap;
    public int dataLen;

    public double beta;
    public double sumLambda;

    public long SUM_LEN = 50_000_000_000L;              // determined by distribution
    public String samplingFile = Paths.get("src/main/dataset/synthetic50M_sampling.csv").toAbsolutePath().toString();

    public ConstantCostModel(int M, QueryParse query, Map<String, Integer> varEventNumMap, EventSchema schema){
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
        networkBandwidth = 0.1;
        int l_t = 20;
        // if benefit >= 0, we can start a new round trip
        double lambda = lambdaMap.get(varName);
        double currentRISLen = bitCount * 1.0 * window / l_t;
        double reducedRatio;
        if(H == 0){
            reducedRatio = headOrTail == 2 ? Math.exp(-2 * lambda * window) : Math.exp(-lambda * window);
        }else{
            reducedRatio = headOrTail == 2 ? Math.exp(-2 * 0.2 * lambda * window) : Math.exp(-0.2 *lambda * window);
        }
        double reducedEvents = reducedRatio * currentRISLen * sumLambda;
        double good = reducedEvents * dataLen / M  / networkBandwidth + beta * reducedEvents;

        double c_p1 = 16.3;
        double c_p2 = 156.5;
        double c_b = 132.5;
        double c_update = 423.4;
        double bad;
        if(H == 0){
            bad = (lambda * SUM_LEN * c_p1) / M +  c_update * currentRISLen * (1 - reducedRatio) * lambda + 2 * (swfSize + bfSize) / networkBandwidth;
        }else{
            bad = (lambda * SUM_LEN * c_p1 + currentRISLen * lambda * c_p2 * H + keyNum * c_b) / M + 2 * (swfSize + bfSize) / networkBandwidth;
        }

        System.out.println("varName: " + varName + "# estimate good:" + (long)good + "ns, " +
                "bad:" + (long)bad + ", benefit: " + (long)(good - bad)+ "ns, " +
                "reducedEvents:" + (int)reducedEvents + ", reducedRatio:" + reducedRatio + ", lambda:" + lambda);

        return good >= bad;
    }
}

/*
public static double c_swf_insertion = 58.8;
public static double c_swf_query = 16.3;
public static double c_wjf_query = 156.5;       // please note that this value is
public static double R = 0.16;
public static double beta = 393.5;
 */