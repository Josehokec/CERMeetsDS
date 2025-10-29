package plan;

import store.EventSchema;

import java.util.Map;

public interface CostEstimator {


    // if benefit >= 0, we can start a new round trip
    boolean newRoundTrip(long bitCount, int headOrTail, String varName, int swfSize, int bfSize, int H, int keyLen, int keyNum, double networkBandwidth);
}
