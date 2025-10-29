package plan;

import engine.NFA;
import engine.SelectionStrategy;
import parser.IndependentPredicate;
import parser.QueryParse;
import store.ColumnInfo;
import store.EventSchema;
import store.FullScan;
import utils.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class MatchCostPredicateModel {
    public List<byte[]> samples = new ArrayList<>(5000);

    public MatchCostPredicateModel(String samplingFile, EventSchema schema){
        File file = new File(samplingFile);
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                byte[] record = schema.covertStringToBytes(line);
                samples.add(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public double getMatchCost(QueryParse query, EventSchema schema) {
        // when choose different engine, we need to reset some parameters
        NFA nfa = new NFA();
        nfa.constructNFA(query);

        Map<String, List<String>> ipMap = query.getIpStringMap();
        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> newIpMap = FullScan.parseIpString(ipMap, schema);
        List<byte[]> filteredSamples = new ArrayList<>();

        for(byte[] sample : samples){
            boolean satisfy = true;
            for(Map.Entry<String, List<Pair<IndependentPredicate, ColumnInfo>>> entry : newIpMap.entrySet()){
                for(Pair<IndependentPredicate, ColumnInfo> ipPair : entry.getValue()){
                    IndependentPredicate ip = ipPair.getKey();
                    ColumnInfo columnInfo = ipPair.getValue();
                    ByteBuffer bb = ByteBuffer.wrap(sample);
                    Object obj = schema.getColumnValue(columnInfo, bb, 0);
                    satisfy = ip.check(obj, columnInfo.getDataType());
                    if(!satisfy){
                        break;
                    }
                }
                if(satisfy){
                    filteredSamples.add(sample);
                    break;
                }
            }
        }

        long startTime = System.nanoTime();
        for(byte[] sample : filteredSamples){
            nfa.consume(sample, SelectionStrategy.AFTER_MATCH_SKIP_TO_NEXT_ROW, schema);
        }
        long endTime = System.nanoTime();
        double totalNs = endTime - startTime + 0.0;
        return totalNs / filteredSamples.size();
    }

//    public static void main(String[] args){
//        String samplingFile = "src/main/dataset/synthetic50M_sampling.csv";
//        EventSchema schema = EventSchema.getEventSchema("SYNTHETIC");
//        MatchCostPredicateModel model = new MatchCostPredicateModel(samplingFile, schema);
//
//        String queryStr = "SELECT * FROM SYNTHETIC MATCH_RECOGNIZE(\n" +
//                "    ORDER BY eventTime\n" +
//                "    MEASURES A.eventTime as ATS, B.eventTime as BTS, C.eventTime as CTS, D.eventTime as DTS\n" +
//                "    ONE ROW PER MATCH\n" +
//                "    AFTER MATCH SKIP TO NEXT ROW\n" +
//                "    PATTERN(A N1*? B N2*? C N3*? D) WITHIN INTERVAL '3' MINUTE\n" +
//                "    DEFINE\n" +
//                "        A AS A.type = 'T_18' AND A.A1 <= 200,\n" +
//                "        B AS B.type = 'T_08' AND B.A1 <= 200 AND B.A3 = A.A3,\n" +
//                "        C AS C.type = 'T_07' AND C.A1 <= 200 AND C.A2 = B.A2,\n" +
//                "        D AS D.type = 'T_01' AND D.A1 <= 200\n" +
//                ");";
//        QueryParse query = new QueryParse(queryStr);
//        double matchCost = model.getMatchCost(query, schema);
//        System.out.println("match cost per record: " + matchCost + " ns");
//    }

}


