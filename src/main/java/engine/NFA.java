package engine;

import parser.DependentPredicate;
import parser.IndependentPredicate;
import parser.QueryParse;
import store.ColumnInfo;
import store.EventSchema;
import utils.Pair;

import java.util.*;

/**
 * Non-determine finite automata for complex event recognition
 * Please note that Flink SQL only support SEQ temporal operator
 * this engine is an optimized version for [SASE] (https://github.com/haopeng/sase)
 */
public class NFA {
    private int stateNum;                           // number of states
    private final HashMap<Integer, State> stateMap;       // all states
    private long window;                            // query window
    private Set<State> activeStates;                // active states
    private final EventCache eventCache;                  // event buffer
    public static boolean withoutDC = false;
    public static String partitionColName = "null";

    public NFA(){
        stateNum = 0;
        stateMap = new HashMap<>();
        activeStates = new TreeSet<>();
        eventCache = new EventCache();
        window = Long.MAX_VALUE;
        State startState = createState("start", true, false);
        activeStates.add(startState);
    }

    /**
     * control this function to generate state
     * @param stateName - state name
     * @param isStart - mark start state
     * @param isFinal - mark final state
     */
    public State createState(String stateName, boolean isStart, boolean isFinal){
        // when create a new state, we need its state name and the length of a match
        State state = new State(stateName, stateNum);

        if(isStart){
            state.setStart();
        }
        if(isFinal){
            state.setFinal();
        }
        // store state, for start state its stateNum is 0
        stateMap.put(stateNum, state);
        stateNum++;
        return state;
    }

    /**
     * according to stateName find target state list
     * @param stateName - state name
     * @return - all states whose name is stateName
     */
    public List<State> getState(String stateName){
        // AND(Type1 a, Type2 b) -> a and b has two states
        List<State> states = new ArrayList<>();
        for (State state : stateMap.values()) {
            if(stateName.equals(state.getStateName())){
                states.add(state);
            }
        }
        return states;
    }

    public List<State> getFinalStates(){
        List<State> finalStates = new ArrayList<>();
        for(State state : stateMap.values()){
            if(state.getIsFinal()){
                finalStates.add(state);
            }
        }
        return finalStates;
    }

    /**
     * based on sql to generate nfa
     * please note that this query only contain seq temporal operator
     * if we want to support AND or OR operators, please modify this function
     * @param query parsed sql statement
     * sql should be correct, i.e., it can be parsed by flink sql
     */
    public void constructNFA(QueryParse query){
        this.window = query.getWindow();
        String tableName = query.getTableName();
        EventSchema schema = EventSchema.getEventSchema(tableName);

        // e.g., PATTERN (A N*? B N*? C) ==> varNames = ['A', 'B', 'C']
        List<String> varNames = query.getVariableNames();
        int varNum = varNames.size();
        // create all states
        for(int i = 0; i < varNum - 1; ++i){
            createState(varNames.get(i), false, false);
        }
        // final state
        createState(varNames.get(varNum - 1), false, true);
        Map<String, List<String>> ipStrMap =  query.getIpStringMap();
        partitionColName = query.getPartitionByColName();
        Map<String, List<DependentPredicate>> dpMap =  query.getDpMap();

        //[4-8]
        if(dpMap.isEmpty()){
            withoutDC = true;
        }

        Set<String> preVarNames = new HashSet<>();
        // add all transaction
        for(int i = 0; i < varNum; ++i){
            String curVarName = varNames.get(i);
            List<String> ipStrList = ipStrMap.get(curVarName);
            List<Pair<IndependentPredicate, ColumnInfo>> ipColInfoPairs = new ArrayList<>(ipStrList.size());

            for(String ipStr : ipStrList){
                IndependentPredicate ip = new IndependentPredicate(ipStr);
                ColumnInfo columnInfo = schema.getColumnInfo(ip.getAttributeName());
                ipColInfoPairs.add(new Pair<>(ip, columnInfo));
            }

            List<DependentPredicate> dpList = new ArrayList<>(8);
            for(String preVarName : preVarNames){
                String key = curVarName.compareTo(preVarName) < 0 ? (curVarName + "-" + preVarName) : (preVarName + "-" + curVarName);
                if(dpMap.get(key) != null){
                    dpList.addAll(dpMap.get(key));
                }
            }
            addTransaction(stateMap.get(i), stateMap.get(i + 1), ipColInfoPairs, dpList);
            preVarNames.add(curVarName);
        }
    }

    public void constructNFA(QueryParse query, Set<String> projectedVarNames){
        this.window = query.getWindow();
        String tableName = query.getTableName();
        EventSchema schema = EventSchema.getEventSchema(tableName);

        List<String> varNames = query.getProjectedVarNames(projectedVarNames);
        int varNum = varNames.size();
        // create all states
        for(int i = 0; i < varNum - 1; ++i){
            createState(varNames.get(i), false, false);
        }
        // final state
        createState(varNames.get(varNum - 1), false, true);
        Map<String, List<String>> ipStrMap =  query.getIpStringMap();
        Map<String, List<DependentPredicate>> dpMap =  query.getDpMap();

        partitionColName = query.getPartitionByColName();

        Set<String> preVarNames = new HashSet<>();
        // add all transaction
        for(int i = 0; i < varNum; ++i){
            String curVarName = varNames.get(i);
            List<String> ipStrList = ipStrMap.get(curVarName);
            List<Pair<IndependentPredicate, ColumnInfo>> ipColInfoPairs = new ArrayList<>(ipStrList.size());

            for(String ipStr : ipStrList){
                IndependentPredicate ip = new IndependentPredicate(ipStr);
                ColumnInfo columnInfo = schema.getColumnInfo(ip.getAttributeName());
                ipColInfoPairs.add(new Pair<>(ip, columnInfo));
            }

            List<DependentPredicate> dpList = new ArrayList<>(8);
            for(String preVarName : preVarNames){
                String key = curVarName.compareTo(preVarName) < 0 ? (curVarName + "-" + preVarName) : (preVarName + "-" + curVarName);
                if(dpMap.get(key) != null){
                    dpList.addAll(dpMap.get(key));
                }
            }
            addTransaction(stateMap.get(i), stateMap.get(i + 1), ipColInfoPairs, dpList);
            preVarNames.add(curVarName);
        }
    }

    /**
     * we use Pair<IndependentPredicate, ColumnInfo> rather than IndependentPredicate
     * because we can quickly obtain the data type and offset, then we can easily obtain
     * the value from the byte record
     * @param curState current state
     * @param nextState next state
     * @param ipList independent predicates and its column information
     * @param dpList dependent predicates
     */
    public void addTransaction(State curState, State nextState, List<Pair<IndependentPredicate, ColumnInfo>> ipList, List<DependentPredicate> dpList){
        Transition transition = new Transition(ipList, dpList, nextState);
        curState.bindTransaction(transition);
    }

    public void consume(byte[] record, SelectionStrategy strategy, EventSchema schema){
        Set<State> allNextStates = new HashSet<>();
        int insertedRecordPointer = -1;
        // last state to first state
        for(State state : activeStates){
            // final state cannot transact
            if(!state.getIsFinal()){
                // using match strategy
                Set<State> nextStates = state.transition(eventCache, record, window, strategy, schema, insertedRecordPointer);
                if(!nextStates.isEmpty()){
                    insertedRecordPointer = eventCache.getAllEvents().size() - 1;
                    allNextStates.addAll(nextStates);
                }
            }
        }
        // add start state, maybe has performance bottle
        activeStates.addAll(allNextStates);
        // activeStates.removeIf(state -> !state.getIsStart() && state.getPartialMatchCache().getMatchList().isEmpty());
    }

    /**
     * please note that this function only suitable the queries that only contain SEQ operator
     * if you want to support and operator, you can choose change argument to string varName
     * @param varName variable name
     * @return related records
     */
    public List<byte[]> getProjectedRecords(String varName){
        List<State> finalStateList = getFinalStates();
        List<Integer> pointers = new ArrayList<>(512);
        Set<Integer> visitPointers = new HashSet<>(512);

        for(State s : finalStateList){
            PartialMatchCache buffer = s.getPartialMatchCache();
            // early break [2025-3-10]
            if(buffer != null) {
                List<String> stateNames = buffer.getStateNames();
                int idx = -1;
                for(int i = 0; i < stateNames.size(); i++){
                    if(varName.equals(stateNames.get(i))){
                        idx = i;
                        break;
                    }
                }
                List<PartialMatch> fullMatches = buffer.getMatchList();
                if(fullMatches != null){
                    for(PartialMatch fullMatch : fullMatches){
                        int p = fullMatch.getPointer(idx);
                        if(!visitPointers.contains(p)){
                            pointers.add(p);
                            visitPointers.add(p);
                        }
                    }
                }
            }
        }
        pointers.sort(null);  // nature sort
        return eventCache.getRecords(pointers);
    }

    // code optimization: 2025-3-10
    public List<byte[]> getProjectedRecords(List<String> processedVarNames){
        List<State> finalStateList = getFinalStates();
        List<Integer> pointers = new ArrayList<>(512);
        Set<Integer> visitPointers = new HashSet<>(512);

        Set<String> names = new HashSet<>(processedVarNames);
        for(State s : finalStateList){
            PartialMatchCache buffer = s.getPartialMatchCache();
            if(buffer != null){
                List<String> stateNames = buffer.getStateNames();
                List<Integer> indexPosList = new ArrayList<>(names.size());
                for(int i = 0; i < stateNames.size(); i++){
                    if(names.contains(stateNames.get(i))){
                        indexPosList.add(i);
                    }
                }
                List<PartialMatch> fullMatches = buffer.getMatchList();
                if(fullMatches != null){
                    for(PartialMatch fullMatch : fullMatches){
                        for(int idx : indexPosList){
                            int p = fullMatch.getPointer(idx);
                            if(!visitPointers.contains(p)){
                                pointers.add(p);
                                visitPointers.add(p);
                            }
                        }
                    }
                }
            }
        }
        pointers.sort(null); // code optimization: 2025-3-10
        return eventCache.getRecords(pointers);
    }

    public List<byte[]> getEvents(){
        return eventCache.getAllEvents();
    }

    public void printAnyMatch(EventSchema schema){
        List<State> finalStateList = getFinalStates();
        int count = 0;
        //System.out.println("Query result:");
        //System.out.println("------------------------");
        for(State s : finalStateList){
            PartialMatchCache buffer = s.getPartialMatchCache();
            if(buffer != null){
                List<PartialMatch> fullMatches = buffer.getMatchList();
                if(fullMatches != null){
                    for(PartialMatch fullMatch : fullMatches){
                        count++;
                        System.out.println(fullMatch.getSingleMatchedResult(eventCache, schema));
                    }
                }
            }
        }
        //System.out.println("------------------------");
        System.out.println("result size: " + count);
    }

    public double getNFASize(){
        double ans = 0;
        for(State state : stateMap.values()){
            PartialMatchCache cache = state.getPartialMatchCache();
            if(cache != null){
                ans += cache.getPartialMatchCacheSize();
            }
        }
        ans += eventCache.getCacheSize();
        return ans;
    }

    public int getMatchesNum(){
        List<State> finalStateList = getFinalStates();
        int count = 0;
        for(State s : finalStateList){
            PartialMatchCache buffer = s.getPartialMatchCache();
            if(buffer != null){
                count += buffer.getMatchCount();
            }
        }
        return count;
    }

    public List<String> getMatchResults(EventSchema schema, SelectionStrategy strategy){
        Set<Integer> accessedPointers = new HashSet<>(128);

        List<String> allMatchedResults = new ArrayList<>(128);
        List<State> finalStateList = getFinalStates();
        for(State s : finalStateList){
            PartialMatchCache buffer = s.getPartialMatchCache();
            if(buffer != null){
                List<PartialMatch> fullMatches = buffer.getMatchList();
                if(fullMatches != null){
                    for(PartialMatch fullMatch : fullMatches){
                        if(strategy == SelectionStrategy.AFTER_MATCH_SKIP_TO_NEXT_ROW){
                            int pointer = fullMatch.getFirstEventPointer();
                            if(!accessedPointers.contains(pointer)){
                                String result = fullMatch.getSingleMatchedResult(eventCache, schema);
                                allMatchedResults.add(result);
                                accessedPointers.add(pointer);
                            }
                        }else{
                            // if we use skip-till-any-match or skip-till-next-match
                            String result = fullMatch.getSingleMatchedResult(eventCache, schema);
                            allMatchedResults.add(result);
                        }
                    }
                }
            }
        }
        return allMatchedResults;
    }

    public void printActiveStates(){
        for(State state : activeStates){
            System.out.println(state);
        }
    }

    public void getFullMatchEventsStatistic(){
        HashMap<String, Set<Integer>> infoMap = new HashMap<>();
        List<State> finalStateList = getFinalStates();
        for(State s : finalStateList){
            PartialMatchCache buffer = s.getPartialMatchCache();
            if(buffer != null){
                List<PartialMatch> fullMatches = buffer.getMatchList();
                if(fullMatches != null){
                    List<String> stateNames = buffer.getStateNames();
                    // initialization
                    for(String stateName : stateNames){
                        if(!infoMap.containsKey(stateName)){
                            infoMap.put(stateName, new HashSet<>(1024));
                        }
                    }
                    // insert pointers
                    int size = stateNames.size();
                    for(PartialMatch fullMatch : fullMatches){
                        List<Integer> pointers = fullMatch.getRecordPointers();
                        for(int i = 0; i < size; ++i){
                            int pointer = pointers.get(i);
                            infoMap.get(stateNames.get(i)).add(pointer);
                        }
                    }
                }
            }
        }

        for(Map.Entry<String,Set<Integer>> entry : infoMap.entrySet()){
            String varName = entry.getKey();
            int size = entry.getValue().size();
            System.out.println("varName: " + varName + " size: " + size);
        }
    }

    public void display(){
        System.out.println("Non-determine finite automata:");
        State initialState = stateMap.get(0);
        // original code can support AND temporal operator
        // System.out.println("--------All NFA paths-------");
        // int cnt = 1;
        for(Transition t : initialState.getTransactions()){
            //System.out.println("path number: " + cnt);
            System.out.println("$ start state: " + initialState);
            //cnt++;
            t.print();
            State nextState = t.getNextState();
            nextState.recursiveDisplayState();
        }
    }
}
