package plan;

import java.util.*;

public class CostModel {
    public static double c_swf_insertion = 58.8;
    public static double c_swf_query = 16.3;
    public static double c_wjf_query = 156.5;       // please note that this value is
    public static double R = 0.16;                  // transmission rate, bits/ns
    public static double beta = 393.5;
    public int M;                                   // number of storage nodes
    public int l_t;                                 // length of slice tag
    public long window;                             // window size
    Map<String, String> varTypeMap;
    Set<String> hasProcessedType;
    public Map<String, Double> lambdaMap;           // varName, arrival rate
    Map<String, Integer> varEventNumMap;
    public double sumLambda;
    public int dataLen;

    public CostModel(String varName0, int l_t, int M, long ell_0, int dataLen, long window, Map<String, Integer> varEventNumMap, Map<String, List<String>> ipMap){
        this.M = M;
        this.l_t = l_t;
        this.dataLen = dataLen;
        this.window = window;
        this.varEventNumMap = varEventNumMap;

        int varNum = varEventNumMap.size();
        varTypeMap = new HashMap<>();
        hasProcessedType = new HashSet<>();

        for(Map.Entry<String, List<String>> entry : ipMap.entrySet()){
            String varName = entry.getKey();
            List<String> ipList = entry.getValue();
            for(String str : ipList){
                int idx = str.indexOf("TYPE = '") + 8;
                if(idx != 7){
                    for(int nextIdx = idx + 1; nextIdx < str.length(); nextIdx++){
                        if(str.charAt(nextIdx) == '\''){
                            String type = str.substring(idx, nextIdx);
                            varTypeMap.put(varName, type);
                            if(varName.equals(varName0)){
                                hasProcessedType.add(type);
                            }
                            break;
                        }
                    }
                    break;
                }
            }
        }

        lambdaMap = new HashMap<>(varNum << 1);
        for(Map.Entry<String, Integer> eventNumEntry : varEventNumMap.entrySet()){
            String varName = eventNumEntry.getKey();
            int eventNum = eventNumEntry.getValue();
            double lambda = eventNum * 1.0 / ell_0;
            lambdaMap.put(varName, lambda);
            sumLambda += lambda;
        }

    }

    public boolean newRoundTrip(long bitCount, int headOrTail, String varName, int swfSize, int bfSize, double bf_selectivity){
        String type = varTypeMap.get(varName);
        if(hasProcessedType.contains(type)){
            return false;
        }else{
            hasProcessedType.add(type);
        }

        // double lambda = lambdaMap.get(varName);
        double lambda = bfSize == 0 ? lambdaMap.get(varName) : lambdaMap.get(varName) * bf_selectivity;

        double currentRISLen = bitCount * 1.0 * window / l_t;
        double reducedRatio = headOrTail == 2 ? Math.exp(-2 * lambda * window) : Math.exp(-lambda * window);
        double reducedEvents = reducedRatio * currentRISLen * sumLambda;

        double l_tsf = reducedEvents * dataLen / M  / R;
        double l_mat = beta * reducedEvents;

        double l_ris_wjf = 2 * (swfSize + bfSize) / R;
        double l_vrf;

        if(bfSize == 0){
            l_vrf = varEventNumMap.get(varName) * c_swf_query + lambda * bitCount * window / l_t * c_swf_insertion;
        }else{
            l_vrf = varEventNumMap.get(varName) * c_swf_query + lambda * bitCount * window / l_t * c_wjf_query  + lambda * bitCount * window / l_t * bf_selectivity * c_swf_insertion;
        }
        l_vrf /= M;

        double benefit = l_tsf + l_mat - l_ris_wjf - l_vrf;
        return benefit >= 0;
    }
}
