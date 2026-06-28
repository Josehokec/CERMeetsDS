package plan;

public class AlwaysOffCostModel implements CostEstimator {

    public AlwaysOffCostModel(int M, parser.QueryParse query, java.util.Map<String, Integer> varEventNumMap, store.EventSchema schema){
    }

    @Override
    public boolean newRoundTrip(long bitCount, int headOrTail, String varName, int swfSize, int bfSize, int H, int keyLen, int keyNum, double networkBandwidth) {
        return false;
    }
}
