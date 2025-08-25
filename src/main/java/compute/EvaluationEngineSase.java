package compute;

import engine.NFA;
import engine.SelectionStrategy;
import parser.QueryParse;
import store.EventSchema;
import java.util.List;

public class EvaluationEngineSase {
    public static void processQuery(List<byte[]> events, String sql){
        long startTime = System.currentTimeMillis();
        QueryParse query = new QueryParse(sql);
        String tableName = query.getTableName();
        EventSchema schema = EventSchema.getEventSchema(tableName);

        NFA nfa = new NFA();
        nfa.constructNFA(query);
        SelectionStrategy strategy = SelectionStrategy.AFTER_MATCH_SKIP_TO_NEXT_ROW;//AFTER_MATCH_SKIP_TO_NEXT_ROW

        for(byte[] event : events){
            nfa.consume(event, strategy, schema);
        }

        List<String> queryRes = nfa.getMatchResults(schema, strategy);

//        for (String queryRe : queryRes) {
//            System.out.println(queryRe);
//        }
        System.out.println("number of tuples: " + queryRes.size());
        long endTime = System.currentTimeMillis();
        System.out.println("match time: " + (endTime - startTime) + "ms");
    }
}
