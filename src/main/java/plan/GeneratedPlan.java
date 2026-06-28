package plan;

import parser.DependentPredicate;
import parser.EqualDependentPredicate;
import parser.QueryParse;

import java.util.*;

public class GeneratedPlan {

    public static List<Map.Entry<String, Integer>> getWeight(Map<String, Integer> varEventNumMap, String headVarName, String tailVarName){
        int varNum = varEventNumMap.size();
        Map<String, Integer> weightMap = new HashMap<>(varNum << 1);
        for(Map.Entry<String, Integer> entry : varEventNumMap.entrySet()){
            String key = entry.getKey();
            int value = entry.getValue();
            if(key.equals(headVarName) || key.equals(tailVarName)){
                weightMap.put(key, value >> 1);
            }else{
                weightMap.put(key, value);
            }
        }

        List<Map.Entry<String, Integer>> sortedEntries = new ArrayList<>(weightMap.entrySet());
        sortedEntries.sort(Map.Entry.comparingByValue());
        return sortedEntries;
    }

    public static Plan basicPlan(Map<String, Integer> varEventNumMap, String headVarName, String tailVarName){
        List<Map.Entry<String, Integer>> sortedEntries = GeneratedPlan.getWeight(varEventNumMap, headVarName, tailVarName);
        Plan plan = new Plan(sortedEntries.size());
        for(Map.Entry<String, Integer> entry : sortedEntries){
            plan.add(entry.getKey());
        }
        return plan;
    }

    // determine the optimal pull push plan is np hard
    // on the one hand, the number of plan is very large
    // on the other hand, we cannot accurately estimate join predicate selectivity
    // here we use heuristic cost evaluation
    public static Plan pullPushPlan(Map<String, Integer> varEventNumMap, Map<String, List<DependentPredicate>> dpMap){
        List<Map.Entry<String, Integer>> list = new ArrayList<>(varEventNumMap.entrySet());
        list.sort(Map.Entry.comparingByValue());
        Set<String> varNames = varEventNumMap.keySet();
        int size = varNames.size();
        Set<String> hasVisitedVarNames = new HashSet<>(size << 1);

        Plan plan = new Plan(size);
        String minVarName = list.get(0).getKey();
        plan.add(minVarName);
        hasVisitedVarNames.add(minVarName);

        // an equal weight is 4, non-equal weight is 1.2,

        double equalWeight = 10;
        double nonEqualWeight = 2.4;

        Set<String> nonVisitedVarNames = new HashSet<>(size << 1);
        for(int i = 1; i < size; i++){
            nonVisitedVarNames.add(list.get(i).getKey());
        }

        for(int i = 1; i < size; i++){
            double maxWeight = 0;
            String optimalVarName = "";
            for(String varName : nonVisitedVarNames) {
                double curWeight = 1;
                for(String preVarName : hasVisitedVarNames){
                    String mapKey = preVarName.compareTo(varName) < 0 ? (preVarName + "-" + varName) : (varName + "-" + preVarName);
                    if (dpMap.containsKey(mapKey)) {
                        List<DependentPredicate> predicates = dpMap.get(mapKey);
                        for (DependentPredicate predicate : predicates) {
                            if (predicate.isEqualDependentPredicate()) {
                                curWeight += equalWeight;
                            } else {
                                curWeight += nonEqualWeight;
                            }
                        }
                    }
                }
                curWeight = curWeight / varEventNumMap.get(varName);
                if(curWeight > maxWeight){
                    optimalVarName = varName;
                    maxWeight = curWeight;
                }
            }
            plan.add(optimalVarName);
            hasVisitedVarNames.add(optimalVarName);
            nonVisitedVarNames.remove(optimalVarName);
        }

        return plan;
    }

    public static Plan filterPlan(Map<String, Integer> varEventNumMap, String headVarName, String tailVarName,
                                  Map<String, List<DependentPredicate>> dpMap){
        // sort
        List<Map.Entry<String, Integer>> list = GeneratedPlan.getWeight(varEventNumMap, headVarName, tailVarName);
        Set<String> varNames = varEventNumMap.keySet();
        int size = varNames.size();
        Set<String> hasVisitedVarNames = new HashSet<>(size << 1);

        Plan plan = new Plan(size);
        String minVarName = list.get(0).getKey();
        plan.add(minVarName);
        hasVisitedVarNames.add(minVarName);

        StringBuilder v = new StringBuilder();
        for(String str : dpMap.keySet()){
            v.append(str);
        }
        String s = v.toString();

        for(int i = 1; i < size; i++){
            String key = list.get(i).getKey();
            if(!s.contains(key)){
                plan.add(key);
                hasVisitedVarNames.add(key);
            }
        }

        double equalWeight = 3;
        Set<String> nonVisitedVarNames = new HashSet<>(size << 1);
        for(int i = 1; i < size; i++){
            String key = list.get(i).getKey();
            if(!hasVisitedVarNames.contains(key)){
                nonVisitedVarNames.add(key);
            }
        }
        for(int i = plan.length(); i < size; i++){
            double maxWeight = 0;
            String optimalVarName = "";
            for(String varName : nonVisitedVarNames) {
                double curWeight = 1;
                for(String preVarName : hasVisitedVarNames){
                    String mapKey = preVarName.compareTo(varName) < 0 ? (preVarName + "-" + varName) : (varName + "-" + preVarName);
                    if (dpMap.containsKey(mapKey)) {
                        List<DependentPredicate> predicates = dpMap.get(mapKey);
                        for (DependentPredicate predicate : predicates) {
                            if (predicate.isEqualDependentPredicate()) {
                                curWeight += equalWeight;
                            }
                        }
                    }
                }
                curWeight = curWeight / varEventNumMap.get(varName);
                if(curWeight > maxWeight){
                    optimalVarName = varName;
                    maxWeight = curWeight;
                }
            }
            plan.add(optimalVarName);
            hasVisitedVarNames.add(optimalVarName);
            nonVisitedVarNames.remove(optimalVarName);
        }

        return plan;
    }

    // this function can generate reasonable plans
    public static List<Plan> getFilterPlans(Map<String, Integer> varEventNumMap, String headVarName, String tailVarName, Map<String, List<String>> eqDpMap){
        int size = varEventNumMap.size();
        List<Map.Entry<String, Integer>> list = GeneratedPlan.getWeight(varEventNumMap, headVarName, tailVarName);
        int priority = 0;
        String[] varNames = new String[size];
        for(Map.Entry<String, Integer> entry : list){
            varNames[priority++] = entry.getKey();
        }
        Plan optimalPlan = new Plan(size);
        // without dp can insert into plan
        StringBuilder v = new StringBuilder();
        for(String str : eqDpMap.keySet()){
            v.append(str);
        }
        String allStr = v.toString();

        optimalPlan.add(varNames[0]);
        for(int col = 1; col < size; col++){
            if(!allStr.contains(varNames[col])){
                optimalPlan.add(varNames[col]);
            }
        }

        List<Plan> ans = new ArrayList<>();
        ans.add(optimalPlan);
        for(int i = optimalPlan.length(); i < size; i++){
            List<Plan> newPlans = new ArrayList<>();
            for(Plan plan : ans){
                boolean isFirst = true;
                for(int k = 0; k < size; k++){
                    if(!plan.hasVarName(varNames[k])){
                        if(isFirst){
                            Plan newPlan = plan.copy();
                            newPlan.add(varNames[k]);
                            newPlans.add(newPlan);
                            isFirst = false;
                        }else{
                            for(String preVarName : plan.getSteps()){
                                String key = varNames[k].compareTo(preVarName) < 0 ? varNames[k] + "-" + preVarName : preVarName + "-" + varNames[k];
                                if(eqDpMap.containsKey(key)){
                                    Plan newPlan = plan.copy();
                                    newPlan.add(varNames[k]);
                                    newPlans.add(newPlan);
                                }
                            }
                        }
                    }
                }
            }
            ans = newPlans;
        }

        return ans;
    }

    public static void main(String[] args){
//        String sql =
//                "SELECT * FROM citibike MATCH_RECOGNIZE(\n" +
//                "    ORDER BY eventTime\n" +
//                "    MEASURES A.ride_id as AID, B.ride_id as BID, C.ride_id AS CID\n" +
//                "    ONE ROW PER MATCH\n" +
//                "    AFTER MATCH SKIP TO NEXT ROW \n" +
//                "    PATTERN (A N1*? B N2*? C N3*? D N4*? E N5*? F N6*? G) WITHIN INTERVAL '30' MINUTE \n" + //
//                "    DEFINE \n" +
//                "    A AS A.type = 'B' AND A.start_lat >= 40.6 AND A.start_lat <= 40.7, \n" +
//                "    B AS B.type = 'F' AND B.start_station_id = A.end_station_id, \n" +
//                "    C AS C.type = 'J' AND C.start_station_id = B.end_station_id\n" +
//                ") MR;";
        String sql =
                "SELECT * FROM citibike MATCH_RECOGNIZE(\n" +
                        "    ORDER BY eventTime\n" +
                        "    MEASURES A.ride_id as AID, B.ride_id as BID, C.ride_id AS CID\n" +
                        "    ONE ROW PER MATCH\n" +
                        "    AFTER MATCH SKIP TO NEXT ROW \n" +
                        "    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '30' MINUTE \n" +
                        "    DEFINE \n" +
                        "    A AS A.type = 'B' AND A.start_lat >= 40.6 AND A.start_lat <= 40.68, \n" +
                        "    B AS B.type = 'F' AND B.start_lat >= 40.6 AND B.start_lat <= 40.68, \n" +
                        "    C AS C.type = 'J' AND C.start_lat >= 40.6 AND C.start_lat <= 40.68\n" +
                        ") MR;";
        QueryParse query = new QueryParse(sql);
        System.out.println("equal dp map" + query.getEqualDpMap());
        Map<String, Integer> varEventNumMap = new HashMap<>();
        varEventNumMap.put("A", 18770);
        varEventNumMap.put("B", 687730);
        varEventNumMap.put("C", 311896);
        //varEventNumMap.put("F", 2011866);
        //varEventNumMap.put("G", 6011891);
        //Plan plan = pullPushPlan(varEventNumMap, query.getDpMap());
        //System.out.println(plan);

        long start = System.nanoTime();
        for(int loop = 0; loop < 10; loop++) {
            //Plan p = filterPlan(varEventNumMap, "A", "C", query.getDpMap());
            Plan p = basicPlan(varEventNumMap, "A", "C");//2945616ns
            System.out.println(p);
        }
        long end = System.nanoTime();

        long averageCost = (end -start)/10;
        System.out.println("cost: " + (averageCost/1000) + "us");

    }

}
