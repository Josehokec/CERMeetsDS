package plan;

import parser.QueryParse;
import store.EventSchema;

import java.util.Map;

public class AlwaysOnCostModel implements CostEstimator {

    public AlwaysOnCostModel(int M, QueryParse query, Map<String, Integer> varEventNumMap, EventSchema schema){
    }


    @Override
    public boolean newRoundTrip(long bitCount, int headOrTail, String varName, int swfSize, int bfSize, int H, int keyLen, int keyNum, double networkBandwidth) {
        return true;
    }
}
