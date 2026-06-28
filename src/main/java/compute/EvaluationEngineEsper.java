package compute;

import com.espertech.esper.client.*;

import event.*;

import java.util.List;

public class EvaluationEngineEsper {
    private static int matchCount = 0;

    public static <T> void processQuery(List<T> events, String epl, String tableName) {
        matchCount = 0;
        long startTime = System.currentTimeMillis();

        Configuration config = new Configuration();

        switch (tableName){
            case "CRIMES":
                config.addEventType("EsperCrimes", EsperCrimes.class);
                break;
            case "CITIBIKE":
                config.addEventType("EsperCitibike", EsperCitibike.class);
                break;
            case "CLUSTER":
                config.addEventType("EsperCluster", EsperCluster.class);
                break;
            default:
                throw new RuntimeException("without this table name: " + tableName);
        }

        EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(config);

        EPStatement statement = epService.getEPAdministrator().createEPL(epl);

        statement.addListener((newData, oldData) -> {
            if (newData != null) {
                matchCount++;
            }
        });

        EPRuntime runtime = epService.getEPRuntime();
        for(T event : events){
            runtime.sendEvent(event);
        }
        epService.destroy();
        System.out.println("number of tuples: " + matchCount);
        long endTime = System.currentTimeMillis();
        System.out.println("match time: " + (endTime - startTime) + "ms");
    }
}
