package parser;

import event.TimeGranularity;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * here we only parse the query semantic that flink sql support
 * details of flink sql with match_recognize keywords please see:
 * <a href="https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/match_recognize/#known-limitations"></a>
 * besides, we require
 * (1) the match_recognize sql contains the window condition;
 * (2) follow by vldb 2023 high performance row pattern recognize using joins, i.e.,
 * PATTERN FORMAT: (varName, irrelevantVarName*?, varName, irrelevantVarName*?, ...),
 * which means we allow skip irrelevant events between two variableName
 * two reasons: distributed, event arrival may unordered
 */
public class QueryParse {
    private boolean unsupportedQuery = false;
    private final String tableName;
    private final List<String> variableNames;
    private final String partitionByColName;
    private long window;
    private TimeGranularity timeGranularity;
    // IndependentPredicate: variableName.attributeName < 10
    private final Map<String, List<String>> ipStringMap = new HashMap<>();
    // DependentPredicate: variableName1.attributeName = variableName2.attributeName
    private final List<String> dpStringList = new ArrayList<>();

    /**
     * obtain tableName, pattern, and predicate constraints
     * we do not process SUM and LAST keywords
     * @param sql - a standard sql statement that contains keyword `MATCH_RECOGNIZE`
     * details can be seen in QueryParseTest.java
     */
    public QueryParse(String sql){
        sql = sql.toUpperCase().replace("\n", "").replace("\r", "");
        Pattern tableNamePattern = Pattern.compile("SELECT\\s+[*]\\s+FROM\\s+(\\w+)\\s*MATCH_RECOGNIZE");
        Matcher tableNameMatcher = tableNamePattern.matcher(sql);
        if (tableNameMatcher.find()) {
            tableName = tableNameMatcher.group(1);
        }else{
            throw new RuntimeException("Wrong sql statement (cannot identify table name), sql: " + sql);
        }

        // Regular expression to match "PARTITION BY" followed by a word
        String regex0 = "PARTITION BY\\s+(\\w+)";
        // Compile the pattern
        Pattern pattern0 = Pattern.compile(regex0);

        // Match the pattern in the input string
        Matcher matcher0 = pattern0.matcher(sql);

        if (matcher0.find()) {
            // Extract the word after "PARTITION BY"
            partitionByColName = matcher0.group(1);
        } else {
            partitionByColName = "null";
        }

        int patternClausePos = sql.indexOf("PATTERN");
        int withinClausePos = sql.indexOf("WITHIN");
        int defineClausePos = sql.indexOf("DEFINE");

        if(patternClausePos == -1 || withinClausePos == -1){
            throw new RuntimeException("Wrong sql statement (without keywords 'PATTERN' or 'WITHIN'), sql: " + sql);
        }

        // please note that we do not process LAST and SUM keywords
        // besides, currently, we do not support OR and BETWEEN keywords
        if(defineClausePos != -1){
            int len = sql.length();
            int defineClauseEnd = sql.length();
            for(int i = len - 1; i > 0; --i){
                char ch = sql.charAt(i);
                if(ch == ')'){
                    defineClauseEnd = i;
                    break;
                }
            }
            // 'DEFINE' has 7 characters
            String defineClauses = sql.substring(defineClausePos + 6, defineClauseEnd);
            String[] defineForVariables = defineClauses.split(",");
            for(String defineForSingleVar : defineForVariables){
                String[] leftAndRight = defineForSingleVar.split(" AS ");
                String varName = leftAndRight[0].trim();

                ipStringMap.put(varName, new ArrayList<>(4));

                if(leftAndRight[1].contains(" BETWEEN ")){
                    // variableName.attributeName BETWEEN a AND b
                    throw new RuntimeException("Currently we cannot support 'A between x and y' syntax, you can change it as 'A >= x AND A <= y'");
                }else{
                    String[] predicates = leftAndRight[1].trim().split(" AND ");
                    // variableName.attributeName [<>!=][=]? value
                    String ipRegex = "[A-Za-z_][A-Za-z0-8_]*[.][A-Za-z_][A-Za-z0-8_]*[\\s+]*([<>!=]=?)\\s*'?(([-]?[0-9]+(.[0-9]+)?)|([A-Za-z0-9_]+))'?";
                    Pattern pattern = Pattern.compile(ipRegex);
                    for(String predicate : predicates){
                        if(!predicate.contains("SUM") && !predicate.contains("LAST")){
                            Matcher matcher = pattern.matcher(predicate.trim());
                            if(matcher.matches()) {
                                // IndependentPredicate ip = new IndependentPredicate(predicate);
                                // ipMap.get(varName).add(ip);
                                ipStringMap.get(varName).add(predicate);
                            }else {
                                dpStringList.add(predicate);
                            }
                        }
                    }
                }
            }
        }

        // PATTERN -> 7 characters
        String patternClause = sql.substring(patternClausePos + 7, withinClausePos).trim();
        // pattern always (variableName[+*][?]?)
        String[] nameArray = patternClause.substring(1, patternClause.length() - 1).split("\\s+");
        int variableNameNum = nameArray.length;
        variableNames = new ArrayList<>((nameArray.length + 1) >> 1);
        // we require even
        if((variableNameNum & 0x01) != 1){
            unsupportedQuery = true;
            return;
        }else{
            for (int i = 0; i < variableNameNum; i++) {
                String name = nameArray[i];
                int strLen = name.length();
                if((i & 0x01) == 1){
                    if(!name.contains("*?") || ipStringMap.containsKey(name.substring(0, strLen - 2))){
                        unsupportedQuery = true;
                        return;
                    }
                }else{
                    // we allow this variable contain {n,m} or +?
                    for(int j = 0; j < strLen; ++j){
                        char ch = name.charAt(j);
                        if(ch == '+' || ch == '{' || ch == '*'){
                            unsupportedQuery = true;
                            return;
                        }
                    }
                    if(!ipStringMap.containsKey(name)){
                        unsupportedQuery = true;
                        return;
                    }
                    variableNames.add(name);
                }
            }
        }

        // parse window
        String[] splits = sql.substring(withinClausePos, defineClausePos).split("\\s+");
        window = Long.parseLong(splits[2].substring(1, splits[2].length() - 1));
        if(splits[3].contains("HOUR")){
            timeGranularity = TimeGranularity.HOUR;
        }else if(splits[3].contains("MINUTE")){
            timeGranularity = TimeGranularity.MINUTER;
        }else if(splits[3].contains("MILLISECOND")){
            timeGranularity = TimeGranularity.MILLISECOND;
        }else if(splits[3].contains("SECOND")){
            timeGranularity = TimeGranularity.SECOND;
        }else{
            timeGranularity = TimeGranularity.SECOND;
        }
    }

    public void print(){
        if(unsupportedQuery){
            System.out.println("we cannot support this query, we more care about allow skip semantic.");
        }else{
            System.out.println("table names: " + tableName);
            System.out.println("variable names: " + variableNames);
            System.out.println("window: " + window + " " + timeGranularity);
            // here we do not process SUM and LAST keywords
            ipStringMap.forEach((k, v) -> System.out.print("variable name: " + k + " ip list: " + v +" "));
            System.out.println("\ndp list: " + dpStringList);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String getPartitionByColName() {
        return partitionByColName;
    }

    public List<String> getVariableNames() {
        return variableNames;
    }

    // please note that we assume that the time granularity of stored event is s
    public long getWindow() {
        long convertedWindow;
        switch (timeGranularity) {
            case HOUR:
                convertedWindow = window * 3600;
                break;
            case MINUTER:
                convertedWindow = window * 60;
                break;
            case SECOND:
                convertedWindow = window;
                break;
            case MILLISECOND:
                convertedWindow = window / 1000;
                break;
            default:
                System.out.println("please note that this query do not give time granularity, we set SECOND");
                convertedWindow = window;
        }

        //If the timestamp of the dataset is not in seconds,
        // please add an if else statement to determine
        if(tableName.equals("CITIBIKE") || tableName.equals("SYNTHETIC")){
            // citibike is ms
            convertedWindow = convertedWindow * 1000;
        } else if (tableName.equals("CLUSTER")) {
            // cluster is us
            convertedWindow = convertedWindow * 1_000_000;
        }

        return convertedWindow;
    }

    public List<String> getProjectedVarNames(Set<String> projectedVarNames){
        List<String> ans = new ArrayList<>(projectedVarNames.size());
        for(String varName : variableNames){
            if(projectedVarNames.contains(varName)){
                ans.add(varName);
            }
        }
        return ans;
    }

    public TimeGranularity getTimeGranularity() {
        return timeGranularity;
    }

    /**
     * 0: head variable, 1: tail variable, 2: middle variable
     * @param variableName variable name
     * @return  head or tail variable marker
     */
    public int headTailMarker(String variableName){
        String firstVariableName = variableNames.get(0);
        if(firstVariableName.equals(variableName)){
            return 0;
        }
        String lastVariableName = variableNames.get(variableNames.size() - 1);
        if(lastVariableName.equals(variableName)){
            return 1;
        }
        return 2;
    }

    public String getHeadVarName(){
        return variableNames.get(0);
    }

    public String getTailVarName(){
        return variableNames.get(variableNames.size() - 1);
    }

    public Map<String, List<String>> getIpStringMap() {
        return ipStringMap;
    }

    public List<String> getDpStringList() {
        return dpStringList;
    }

    public boolean compareSequence(String hasVisitVarName, String varName){
        // false: previous, true: next
        for(String str : variableNames){
            if(str.equals(hasVisitVarName)){
                return false;
            }
            if(str.equals(varName)){
                return true;
            }
        }
        System.out.println("wrong state, maybe has wrong variable name");
        return false;
    }

    /**
     * note that users may give: V1.BEAT = V2.BEAT + 1 AND V2.DISTRICT = V1.DISTRICT
     * to avoid generate two keys, we define key = varName_{x}-varName_{y}, where x < y
     * @return varName_{x}-varName_{y}, predicates
     */
    public Map<String, List<String>> getEqualDpMap(){
        Map<String, List<String>> dpMap = new HashMap<>(8);
        for(String dpStr : dpStringList){
            int pos = dpStr.indexOf('=');
            if(pos != -1){
                char preChar = dpStr.charAt(pos - 1);
                if(preChar != '!' && preChar != '<' && preChar != '>'){
                    // this predicate should be equal predicate
                    DependentPredicate dp = new EqualDependentPredicate(dpStr);
                    String varName1 = dp.getLeftVarName();
                    String varName2 = dp.getRightVarName();
                    String key = varName1.compareTo(varName2) < 0 ? (varName1 + "-" + varName2) : (varName2 + "-" + varName1);
                    if(dpMap.containsKey(key)){
                        dpMap.get(key).add(dpStr);
                    } else{
                        List<String> predicates = new ArrayList<>(4);
                        predicates.add(dpStr);
                        dpMap.put(key, predicates);
                    }
                }
            }
        }
        return dpMap;
    }

    // used for two trips
    public Set<String> getVarAttrSetFromDpList(){
        Set<String> ans = new HashSet<>(dpStringList.size() << 2);
        for(String dpStr : dpStringList){
            int pos = dpStr.indexOf('=');
            if(pos != -1){
                char preChar = dpStr.charAt(pos - 1);
                if(preChar != '!' && preChar != '<' && preChar != '>'){
                    // this predicate should be equal predicate
                    DependentPredicate dp = new EqualDependentPredicate(dpStr);
                    String varName1 = dp.getLeftVarName();
                    String attrName1 = dp.getLeftAttrName();
                    ans.add(varName1 + "-" + attrName1);

                    String varName2 = dp.getRightVarName();
                    String attrName2 = dp.getRightAttrName();
                    ans.add(varName2 + "-" + attrName2);
                }
            }
        }
        return ans;
    }


    // format: key<A-C> value<A.beat=C.beat>
    public Map<String, List<DependentPredicate>> getDpMap(){
        Map<String, List<DependentPredicate>> dpMap = new HashMap<>(8);
        for(String dpStr : dpStringList){
            int pos = dpStr.indexOf('=');
            boolean isEqualDp = false;
            DependentPredicate dp = null;
            if(pos != -1){
                char preChar = dpStr.charAt(pos - 1);
                if(preChar != '!' && preChar != '<' && preChar != '>'){
                    isEqualDp = true;
                    dp = new EqualDependentPredicate(dpStr);
                }
            }
            if(!isEqualDp){
                dp = new NoEqualDependentPredicate(dpStr);
            }
            String varName1 = dp.getLeftVarName();
            String varName2 = dp.getRightVarName();
            String key = varName1.compareTo(varName2) < 0 ? (varName1 + "-" + varName2) : (varName2 + "-" + varName1);
            if(dpMap.containsKey(key)){
                dpMap.get(key).add(dp);
            } else{
                List<DependentPredicate> predicates = new ArrayList<>(4);
                predicates.add(dp);
                dpMap.put(key, predicates);
            }
        }
        return dpMap;
    }

    public boolean isUnsupportedQuery() {
        return unsupportedQuery;
    }
}
