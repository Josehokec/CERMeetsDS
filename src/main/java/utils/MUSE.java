package utils;

import engine.NFA;
import engine.SelectionStrategy;
import request.ReadQueries;
import parser.IndependentPredicate;
import parser.QueryParse;
import store.ColumnInfo;
import store.EventSchema;
import utils.Pair;
import utils.SortByTs;

import java.io.*;
import java.nio.ByteBuffer;

import java.util.*;

/**
 * estimate muse transmission cost
 */
public class MUSE {
    private static String prefixStr = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator;
    private static Map<String, Integer> varEventNum = new HashMap<>();

    // return a map, key is varName, value is byte event list
    public static List<byte[]> readFile(String fileName, EventSchema schema, Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap){
        varEventNum.clear();

        List<byte[]> res = new ArrayList<>(1024);
        String filePath = prefixStr + "dataset" + File.separator + fileName;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                byte[] events = schema.covertStringToBytes(line);

                for(Map.Entry<String, List<Pair<IndependentPredicate, ColumnInfo>>> entry : ipMap.entrySet()) {
                    String varName = entry.getKey();
                    List<Pair<IndependentPredicate, ColumnInfo>> ips = entry.getValue();
                    boolean satisfied = true;
                    for(Pair<IndependentPredicate, ColumnInfo> pair : ips){
                        IndependentPredicate ip = pair.getKey();
                        ColumnInfo columnInfo = pair.getValue();
                        ByteBuffer buffer = ByteBuffer.wrap(events);
                        Object obj = schema.getColumnValue(columnInfo, buffer, 0);
                        if(!ip.check(obj, columnInfo.getDataType())){
                            satisfied = false;
                            break;
                        }
                    }
                    if(satisfied){
                        if(varEventNum.containsKey(varName)){
                            varEventNum.put(varName, varEventNum.get(varName) + 1);
                        }else{
                            varEventNum.put(varName, 1);
                        }
                        res.add(events);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return res;
    }

    public static Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> parseIpString(Map<String, List<String>> ipStringMap, EventSchema schema) {
        Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap = new HashMap<>(ipStringMap.size() << 2);
        // parse independent predicates
        for(String varName : ipStringMap.keySet()){
            List<String> ipStringList = ipStringMap.get(varName);
            List<Pair<IndependentPredicate, ColumnInfo>> pairs = new ArrayList<>(ipStringList.size());
            for (String ipString : ipStringList){
                IndependentPredicate ip = new IndependentPredicate(ipString);
                pairs.add(new Pair<>(ip, schema.getColumnInfo(ip.getAttributeName())));
            }
            ipMap.put(varName, pairs);
        }
        return ipMap;
    }

    public static List<List<String>> permute(List<String> nums) {
        List<List<String>> result = new ArrayList<>();
        backtrack(result, new ArrayList<>(), nums);
        return result;
    }

    private static void backtrack(List<List<String>> result, List<String> tempList, List<String> nums) {
        if (tempList.size() == nums.size()) {
            result.add(new ArrayList<>(tempList));
        } else {
            for (int i = 0; i < nums.size(); i++) {
                if (tempList.contains(nums.get(i))) continue; // 跳过已使用的元素
                tempList.add(nums.get(i));
                backtrack(result, tempList, nums);
                tempList.remove(tempList.size() - 1); // 回溯
            }
        }
    }

    public static void testCrimes(int nodeNum) throws FileNotFoundException{
        List<String> sqlList = ReadQueries.getCrimesQueryList();
        for(int i = 0; i < sqlList.size(); i++) {
            System.out.println("query id: " + i);
            String sql = sqlList.get(i);
            // System.out.println(sql);
            long startTime = System.currentTimeMillis();
            QueryParse query = new QueryParse(sql);
            String tableName = query.getTableName();

            EventSchema schema = EventSchema.getEventSchema(tableName);
            long dataLen = schema.getFixedRecordLen();

            Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap =
                    parseIpString(query.getIpStringMap(), schema);

            String fileName = "crimes.csv";
            List<byte[]> filteredEvents = readFile(fileName, schema, ipMap);
            System.out.println(varEventNum);

            List<String> varNames = new ArrayList<>(varEventNum.keySet());
            List<List<String>> result = permute(varNames);

            List<byte[]> byteRecords = SortByTs.sort(filteredEvents, schema);

            long minTransmission = Long.MAX_VALUE;
            for(List<String> seq : result){
                // case 1: 1 -> 2 -> 3
                {
                    long sumTransmission = 0;
                    // transmission cost
                    sumTransmission += (nodeNum - 1) * varEventNum.get(varNames.get(0)) * dataLen;
                    // match for two variables
                    NFA nfa = new NFA();
                    Set<String> twoVars = new HashSet<>();
                    twoVars.add(seq.get(0));
                    twoVars.add(seq.get(1));
                    nfa.constructNFA(query, twoVars);

                    for(byte[] event : byteRecords){
                        nfa.consume(event, SelectionStrategy.SKIP_TILL_ANY_MATCH, schema);
                    }
                    // number of partial matches
                    int matchesNum = nfa.getMatchesNum();
                    sumTransmission += 2 * dataLen * matchesNum;

                    // get the minimum cost
                    if(sumTransmission < minTransmission){
                        minTransmission = sumTransmission;
                        System.out.println("process sequence: " + seq.get(0) + " -> " + seq.get(1) + " -> " + seq.get(2));
                    }
                }
                // case 2: 1, 2 -> 3
                {
                    long sumTransmission = 0;
                    // calculate
                    sumTransmission += (nodeNum - 1) * varEventNum.get(varNames.get(0)) * dataLen;
                    sumTransmission += (nodeNum - 1) * varEventNum.get(varNames.get(1)) * dataLen;

                    // please note that we do not gather all results
                    if(sumTransmission < minTransmission){
                        minTransmission = sumTransmission;
                        System.out.println("process sequence: [" + seq.get(0) + "," + seq.get(1) + "] -> " + seq.get(2));
                    }
                }
            }
            System.out.println("minimum transmission cost: " + minTransmission + " Byte");
            long endTime = System.currentTimeMillis();
            System.out.println("elapsed time: " + (endTime - startTime) + " ms");
        }
    }

    public static void testCitibike(int nodeNum) throws FileNotFoundException {
        //PrintStream printStream = new PrintStream(prefixStr + "output" + File.separator + "MUSE_citibike.txt");
        //System.setOut(printStream);

        List<String> sqlList = ReadQueries.getCitibikeQueryList();

        for(int i = 0; i < sqlList.size(); i++) {
            System.out.println("query id: " + i);
            String sql = sqlList.get(i);
            // System.out.println(sql);
            long startTime = System.currentTimeMillis();
            QueryParse query = new QueryParse(sql);
            String tableName = query.getTableName();

            EventSchema schema = EventSchema.getEventSchema(tableName);
            long dataLen = schema.getFixedRecordLen();

            Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap =
                    parseIpString(query.getIpStringMap(), schema);

            String fileName = "citibike.csv";
            List<byte[]> filteredEvents = readFile(fileName, schema, ipMap);
            System.out.println(varEventNum);

            List<String> varNames = new ArrayList<>(varEventNum.keySet());
            List<List<String>> result = permute(varNames);

            List<byte[]> byteRecords = SortByTs.sort(filteredEvents, schema);

            long minTransmission = Long.MAX_VALUE;
            for(List<String> seq : result){
                // case 1: 1 -> 2 -> 3
                {
                    long sumTransmission = 0;
                    // transmission cost
                    sumTransmission += (nodeNum - 1) * varEventNum.get(varNames.get(0)) * dataLen;
                    // match for two variables
                    NFA nfa = new NFA();
                    Set<String> twoVars = new HashSet<>();
                    twoVars.add(seq.get(0));
                    twoVars.add(seq.get(1));
                    nfa.constructNFA(query, twoVars);

                    for(byte[] event : byteRecords){
                        nfa.consume(event, SelectionStrategy.SKIP_TILL_ANY_MATCH, schema);
                    }
                    // number of partial matches
                    int matchesNum = nfa.getMatchesNum();
                    sumTransmission += 2 * dataLen * matchesNum;

                    // get the minimum cost
                    if(sumTransmission < minTransmission){
                        minTransmission = sumTransmission;
                        System.out.println("process sequence: " + seq.get(0) + " -> " + seq.get(1) + " -> " + seq.get(2));
                    }
                }
                // case 2: 1, 2 -> 3
                {
                    long sumTransmission = 0;
                    // calculate
                    sumTransmission += (nodeNum - 1) * varEventNum.get(varNames.get(0)) * dataLen;
                    sumTransmission += (nodeNum - 1) * varEventNum.get(varNames.get(1)) * dataLen;

                    // please note that we do not gather all results
                    if(sumTransmission < minTransmission){
                        minTransmission = sumTransmission;
                        System.out.println("process sequence: [" + seq.get(0) + "," + seq.get(1) + "] -> " + seq.get(2));
                    }
                }
            }
            System.out.println("minimum transmission cost: " + minTransmission + " Byte");
            long endTime = System.currentTimeMillis();
            System.out.println("elapsed time: " + (endTime - startTime) + " ms");
        }
    }

    public static void testCluster(int nodeNum) throws FileNotFoundException {
        // cluster dataset
        List<String> sqlList = ReadQueries.getClusterQueryList();

        for(int i = 0; i < sqlList.size(); i++) {
            System.out.println("query id: " + i);
            String sql = sqlList.get(i);
            // System.out.println(sql);
            long startTime = System.currentTimeMillis();
            QueryParse query = new QueryParse(sql);
            String tableName = query.getTableName();

            EventSchema schema = EventSchema.getEventSchema(tableName);
            long dataLen = schema.getFixedRecordLen();

            Map<String, List<Pair<IndependentPredicate, ColumnInfo>>> ipMap =
                    parseIpString(query.getIpStringMap(), schema);

            String fileName = "cluster.csv";
            List<byte[]> filteredEvents = readFile(fileName, schema, ipMap);
            System.out.println(varEventNum);

            List<String> varNames = new ArrayList<>(varEventNum.keySet());
            List<List<String>> result = permute(varNames);

            List<byte[]> byteRecords = SortByTs.sort(filteredEvents, schema);

            long minTransmission = Long.MAX_VALUE;
            for(List<String> seq : result){
                // case 1: 1 -> 2 -> 3
                {
                    long sumTransmission = 0;
                    // transmission cost
                    sumTransmission += (nodeNum - 1) * varEventNum.get(varNames.get(0)) * dataLen;
                    // match for two variables
                    NFA nfa = new NFA();
                    Set<String> twoVars = new HashSet<>();
                    twoVars.add(seq.get(0));
                    twoVars.add(seq.get(1));
                    nfa.constructNFA(query, twoVars);

                    for(byte[] event : byteRecords){
                        nfa.consume(event, SelectionStrategy.SKIP_TILL_ANY_MATCH, schema);
                    }
                    // number of partial matches
                    int matchesNum = nfa.getMatchesNum();
                    sumTransmission += 2 * dataLen * matchesNum;

                    // get the minimum cost
                    if(sumTransmission < minTransmission){
                        minTransmission = sumTransmission;
                        System.out.println("process sequence: " + seq.get(0) + " -> " + seq.get(1) + " -> " + seq.get(2));
                    }
                }
                // case 2: 1, 2 -> 3
                {
                    long sumTransmission = 0;
                    // calculate
                    sumTransmission += (nodeNum - 1) * varEventNum.get(varNames.get(0)) * dataLen;
                    sumTransmission += (nodeNum - 1) * varEventNum.get(varNames.get(1)) * dataLen;

                    // please note that we do not gather all results
                    if(sumTransmission < minTransmission){
                        minTransmission = sumTransmission;
                        System.out.println("process sequence: [" + seq.get(0) + "," + seq.get(1) + "] -> " + seq.get(2));
                    }
                }
            }
            System.out.println("minimum transmission cost: " + minTransmission + " Byte");
            long endTime = System.currentTimeMillis();
            System.out.println("elapsed time: " + (endTime - startTime) + " ms");
        }
    }

    public static void main(String[] args) throws FileNotFoundException {
        int nodeNum = 4;
        // testCrimes(nodeNum);
        // testCitibike(nodeNum);
        testCluster(nodeNum);
    }
}

