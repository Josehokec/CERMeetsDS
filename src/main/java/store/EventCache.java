package store;


import filter.*;
import parser.EqualDependentPredicate;
import rpc.NaiveFilterBasedRPCImpl;
import rpc.iface.TsAttrPair;
import utils.Pair;
import utils.ReplayIntervals;

import java.nio.ByteBuffer;
import java.util.*;


/**
 * cache the events that maybe involved in matching
 * note that we assume that all related events can be stored in caches
 * in this class, we define many function for different methods
 */
public class EventCache {
    private final EventSchema schema;
    List<byte[]> records;
    Map<String, List<Integer>> varPointers;

    public EventCache(EventSchema schema, List<byte[]> records, Map<String, List<Integer>> varPointers) {
        this.schema = schema;
        //this.cacheRecordNum = cacheRecordNum;
        this.records = records;
        this.varPointers = varPointers;
    }

    public EventSchema getSchema(){
        return schema;
    }

    // this function is used by push-pull method
    public void sortByTimestamp(){
        Set<String> varNames = varPointers.keySet();
        for(String varName : varNames){
            List<Integer> pointers = varPointers.get(varName);
            int size = pointers.size();
            List<Pair<Long, Integer>> pairs = new ArrayList<>(size);

            for(int p : pointers){
                byte[] record = records.get(p);
                long ts = schema.getTimestamp(record);
                pairs.add(new Pair<>(ts, p));
            }
            pairs.sort(Comparator.comparingLong(Pair::getKey));

            List<Integer> newPointers = new ArrayList<>(size);
            for(Pair<Long, Integer> pair : pairs){
                newPointers.add(pair.getValue());
            }
            varPointers.put(varName, newPointers);
        }
    }

    public double getCacheSize(){
        double ans = records.size() * schema.getFixedRecordLen();
        for(List<Integer> value : varPointers.values()){
            ans += value.size() * 4;
        }
        return ans;
    }

    // this function is used for updating cache
    public List<Integer> getMergedPointers(){
        List<Integer> mergedPointers = new ArrayList<>(8);
        for(List<Integer> pointers : varPointers.values()){
            mergedPointers = merge(mergedPointers, pointers);
        }
        return mergedPointers;
    }

    public int getEventNum(String varName){
        return varPointers.get(varName).size();
    }

    // this function is used by push-pull method
    public void cleanByName(String varName){
        List<Integer> pointers = varPointers.get(varName);
        for(Integer pointer : pointers){
            records.set(pointer, null);
        }
        varPointers.remove(varName);
    }

    public int getRecordSize(){
        return schema.getFixedRecordLen();
    }

    public List<byte[]> getVarByteRecords(String varName, int offset){
        List<Integer> pointers = varPointers.get(varName);
        List<byte[]> ans = new ArrayList<>(Math.max(8, pointers.size() - offset));
        for(int i = offset; i < pointers.size(); i++){
            ans.add(records.get(pointers.get(i)));
        }
        return ans;
    }

    public List<byte[]> mergeEvents(List<byte[]> allEvents, List<byte[]> curEvents){
        int size2 = curEvents.size();
        if(allEvents.isEmpty()){
            return curEvents;
        }else{
            int size1 = allEvents.size();
            List<byte[]> mergedList = new ArrayList<>(size1 + size2);
            int i = 0, j = 0;
            while(i < size1 && j < size2){
                byte[] record1 = allEvents.get(i);
                long ts1 = schema.getTimestamp(record1);

                byte[] record2 = curEvents.get(j);
                long ts2 = schema.getTimestamp(record2);
                if(ts1 <= ts2){
                    mergedList.add(record1);
                    i++;
                }else {
                    mergedList.add(record2);
                    j++;
                }
            }

            while(i < size1){
                mergedList.add(allEvents.get(i));
                i++;
            }

            while(j < size2){
                mergedList.add(curEvents.get(j));
                j++;
            }
            return mergedList;
        }
    }

    public Map<String, Integer> getCardinality(){
        Map<String, Integer> cardinalityMap = new HashMap<>(varPointers.size() << 1);
        for(String varName : varPointers.keySet()){
            cardinalityMap.put(varName, varPointers.get(varName).size());
        }
        return cardinalityMap;
    }

    // we choose the variable with the lowest selectivity to generate replay interval
    public ByteBuffer generateReplayIntervals(String varName, long window, int headTailMarker){
        // 0: head variable, 1: tail variable, 2: middle variable
        List<Integer> pointers = varPointers.get(varName);
        ReplayIntervals intervals = new ReplayIntervals(pointers.size());

        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;

        if(NaiveFilterBasedRPCImpl.enableInOrderInsert){
            for(Integer pointer : pointers){
                byte[] record = records.get(pointer);
                long ts = schema.getTimestamp(record);
                intervals.insertInOrder(ts + leftOffset, ts + rightOffset);
            }
        }else{
            for(Integer pointer : pointers){
                byte[] record = records.get(pointer);
                long ts = schema.getTimestamp(record);
                intervals.insert(ts + leftOffset, ts + rightOffset);
            }
        }
        return intervals.serialize();
    }

    public Set<TsAttrPair> getTsAttrPairSet(String varName, String attrName){
        List<Integer> pointers = varPointers.get(varName);
        Set<TsAttrPair> ans = new HashSet<>(pointers.size() << 1);
        // in fact, if existing multiple equal predicate condition, we can combine two attributes
        for(Integer pointer : pointers) {
            byte[] record = records.get(pointer);
            long timestamp = schema.getTimestamp(record);
            // to accelerate bf generation, we do not consider the case that X.A=Y.A+1
            Object obj = schema.getColumnValue(attrName, record);
            ans.add(new TsAttrPair(timestamp, obj.toString()));
        }
        return ans;
    }

    public Set<rpc2.iface.TsAttrPair> getTsAttrPairSet2(String varName, String attrName){
        List<Integer> pointers = varPointers.get(varName);
        Set<rpc2.iface.TsAttrPair> ans = new HashSet<>(pointers.size() << 1);
        // in fact, if existing multiple equal predicate condition, we can combine two attributes
        for(Integer pointer : pointers) {
            byte[] record = records.get(pointer);
            long timestamp = schema.getTimestamp(record);
            // to accelerate bf generation, we do not consider the case that X.A=Y.A+1
            Object obj = schema.getColumnValue(attrName, record);
            ans.add(new rpc2.iface.TsAttrPair(timestamp, obj.toString()));
        }
        return ans;
    }

    public ByteBuffer generateBloomFilter(String varName, int keyNum, long window, List<EqualDependentPredicate> dps){
        double DEFAULT_FPR = 0.01;
        // WindowWiseBF or StandardBF
        BasicBF bf = new WindowWiseBF(DEFAULT_FPR, keyNum);
        List<Integer> pointers = varPointers.get(varName);

        // in fact, if existing multiple equal predicate condition, we can combine two attributes
        EqualDependentPredicate firstEDP = dps.get(0);
        for(Integer pointer : pointers){
            byte[] record = records.get(pointer);
            long timestamp = schema.getTimestamp(record);
            // to accelerate bf generation, we do not consider the case that X.A=Y.A+1
            String attrName = firstEDP.getAttrName(varName);
            Object obj = schema.getColumnValue(attrName, record);
            bf.insert(obj.toString(), timestamp / window);
        }
        return bf.serialize();
    }

    public Map<Long, Set<String>> generatePairSet(String varName, long window, List<EqualDependentPredicate> dps){
        Map<Long, Set<String>> pairSet = new HashMap<>();

        List<Integer> pointers = varPointers.get(varName);

        // in fact, if existing multiple equal predicate condition, we can combine two attributes
        EqualDependentPredicate firstEDP = dps.get(0);
        for(Integer pointer : pointers){
            byte[] record = records.get(pointer);
            long timestamp = schema.getTimestamp(record);
            // to accelerate bf generation, we do not consider the case that X.A=Y.A+1
            String attrName = firstEDP.getAttrName(varName);
            String attrValue = schema.getColumnValue(attrName, record).toString();

            pairSet.computeIfAbsent(timestamp / window, k -> new HashSet<>()).add(attrValue);
        }
        return pairSet;
    }

    public void simpleFilter(String varName, long window, SWF swf){
        List<Integer> pointers = varPointers.get(varName);
        List<Integer> updatedPointers = new ArrayList<>(pointers.size());
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(swf.query(ts, window)){
                updatedPointers.add(pointer);
            }
        }
        // System.out.println("join preparation, [" + varName + "] original size: " + pointers.size() + " -> updated size:" + updatedPointers.size());//debug
        varPointers.put(varName, updatedPointers);
    }

    public void simpleFilter(String varName, long window, OptimizedSWF optimizedSWF){
        List<Integer> pointers = varPointers.get(varName);
        List<Integer> updatedPointers = new ArrayList<>(pointers.size() >> 1);
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(optimizedSWF.query(ts, window)){
                updatedPointers.add(pointer);
            }
        }
        // System.out.println("join preparation, [" + varName + "] original size: " + pointers.size() + " -> updated size:" + updatedPointers.size());//debug
        varPointers.put(varName, updatedPointers);
    }

    public void simpleFilter(String varName, ReplayIntervals intervals){
        List<Integer> pointers = varPointers.get(varName);
        List<Integer> updatedPointers = new ArrayList<>(pointers.size() >> 1);
        for(int pointer : pointers) {
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(intervals.contains(ts)){
                updatedPointers.add(pointer);
            }
        }
        // System.out.println("join preparation, [" + varName + "] original size: " + pointers.size() + " -> updated size:" + updatedPointers.size());//debug
        varPointers.put(varName, updatedPointers);
    }

    public ByteBuffer updatePointers(String varName, long window, int headTailMarker, OptimizedSWF optimizedSWF){
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;

        List<Integer> pointers = varPointers.get(varName);
        // suppose we can filter quarter of events
        List<Integer> updatedPointers = new ArrayList<>(pointers.size() >> 2);
        UpdatedMarkers updatedMarkers = new UpdatedMarkers(optimizedSWF.getBucketNum());

        // when events arrival in ordered, we can speedup filtering
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(optimizedSWF.query(ts, window)){
                updatedPointers.add(pointer);
                updateShrinkFilter(ts + leftOffset, ts + rightOffset, window, optimizedSWF, updatedMarkers);
            }else{
                records.set(pointer, null);
            }
        }

        varPointers.put(varName, updatedPointers);
        return updatedMarkers.serialize();
    }

    public ByteBuffer updatePointers(String varName, long window, int headTailMarker, SWF swf){
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;

        List<Integer> pointers = varPointers.get(varName);
        List<Integer> updatedPointers = new ArrayList<>(pointers.size());

        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(swf.query(ts, window)){
                swf.updateRange(ts + leftOffset, ts + rightOffset, window);
                updatedPointers.add(pointer);
            }
        }

        varPointers.put(varName, updatedPointers);
        return swf.serialize();
    }

    public ByteBuffer updatePointers2(String varName, long window, int headTailMarker, ReplayIntervals intervals){
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;

        ReplayIntervals updatedIntervals = new ReplayIntervals();

        List<Integer> pointers = varPointers.get(varName);
        List<Integer> updatedPointers = new ArrayList<>(pointers.size());

        for(int pointer : pointers) {
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(intervals.contains(ts)){
                updatedIntervals.insertInOrder(ts + leftOffset, ts + rightOffset);
                updatedPointers.add(pointer);
            }
        }

        varPointers.put(varName, updatedPointers);
        updatedIntervals.intersect(intervals);

        return updatedIntervals.serialize();
    }

    // used for UpdatedMarkers
    public static void updateShrinkFilter(long startTs, long endTs, long window, OptimizedSWF swf, UpdatedMarkers updatedMarkers){
        long startWindowId = startTs / window;
        long endWindowId = endTs / window;
        int leftMarker = OptimizedSWF.getLeftIntervalMarker(startTs, startWindowId * window, window);
        int rightMarker = OptimizedSWF.getRightIntervalMarker(endTs, endWindowId * window, window);
        int leftPosition = swf.queryWindowId(startWindowId);
        if(leftPosition != 0x3){
            updatedMarkers.update(leftPosition >>> 2, leftPosition & 0x1, leftMarker);
        }

        int rightPosition = swf.queryWindowId(endWindowId);
        if(rightPosition != 0x3){
            updatedMarkers.update(rightPosition >>> 2, rightPosition & 0x1, rightMarker);
        }
        for(long wid = startWindowId + 1; wid < endWindowId; wid++){
            int position = swf.queryWindowId(wid);
            if(position != 0x3){
                updatedMarkers.update(position >>> 2, position & 0x1, 0xfffff);
            }
        }
    }

    public ByteBuffer updatePointers(String varName, long window, int headTailMarker, OptimizedSWF swf, Map<String, Boolean> previousOrNext,
                                     Map<String, BasicBF> bfMap, Map<String, List<EqualDependentPredicate>> dependentPredicateMap){
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;
        UpdatedMarkers updatedMarkers = new UpdatedMarkers(swf.getBucketNum());
        List<Integer> pointers = varPointers.get(varName);

        List<Integer> updatedPointers = new ArrayList<>(pointers.size());

        int cnt = 0;
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);

            if(swf.query(ts, window)){
                cnt++;
                // check all related dependent predicates
                // for example, varName: c, previousVariables: a, b
                // dependent predicates: a.A1 = c.A1 + 5 AND b.A2 * 2 = c.A2
                boolean satisfied = true;
                for(String preVarName : previousOrNext.keySet()){
                    List<EqualDependentPredicate> dps = dependentPredicateMap.get(preVarName);
                    BasicBF lbf = bfMap.get(preVarName);
                    EqualDependentPredicate firstEDP = dps.get(0);
                    String attrName = firstEDP.getAttrName(varName);
                    Object obj = schema.getColumnValue(attrName, record);
                    String key = obj.toString();

                    // false: previous, true: next
                    boolean next = previousOrNext.get(preVarName);
                    if(next){
                        if(!lbf.contain(key, ts / window) && !lbf.contain(key, ts / window + 1)){
                            satisfied = false;
                            break;
                        }
                    }else{
                        if(!lbf.contain(key, ts/ window) && !lbf.contain(key, ts/ window - 1)){
                            satisfied = false;
                            break;
                        }
                    }
                }

                // if satisfy window condition and join condition
                if(satisfied){
                    updatedPointers.add(pointer);
                    updateShrinkFilter(ts + leftOffset, ts + rightOffset, window, swf, updatedMarkers);
                }
            }
        }

        // System.out.println("varName: " + varName + " original number of events:" + cnt + " -> filteredEventNum: " + updatedPointers.size());

        varPointers.put(varName, updatedPointers);
        return updatedMarkers.serialize();
    }

    public ByteBuffer updatePointers(String varName, long window, int headTailMarker, SWF swf, Map<String, Boolean> previousOrNext,
                                     Map<String, BasicBF> bfMap, Map<String, List<EqualDependentPredicate>> dependentPredicateMap){
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;

        List<Integer> pointers = varPointers.get(varName);
        List<Integer> updatedPointers = new ArrayList<>(pointers.size());

        int cnt = 0;
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);

            if(swf.query(ts, window)){
                cnt++;
                // check all related dependent predicates
                // for example, varName: c, previousVariables: a, b
                // dependent predicates: a.A1 = c.A1 + 5 AND b.A2 * 2 = c.A2
                boolean satisfied = true;
                for(String preVarName : previousOrNext.keySet()){

                    List<EqualDependentPredicate> dps = dependentPredicateMap.get(preVarName);
                    BasicBF lbf = bfMap.get(preVarName);
                    EqualDependentPredicate firstEDP = dps.get(0);
                    String attrName = firstEDP.getAttrName(varName);
                    Object obj = schema.getColumnValue(attrName, record);
                    String key = obj.toString();

                    // false: previous, true: next
                    boolean next = previousOrNext.get(preVarName);
                    if(next){
                        if(!lbf.contain(key, ts / window) && !lbf.contain(key, ts / window + 1)){
                            satisfied = false;
                            break;
                        }
                    }else{
                        if(!lbf.contain(key, ts/ window) && !lbf.contain(key, ts/ window - 1)){
                            satisfied = false;
                            break;
                        }
                    }
                }
                // if satisfy window condition and join condition
                if(satisfied){
                    updatedPointers.add(pointer);
                    swf.updateRange(ts + leftOffset, ts + rightOffset, window);
                }
            }
        }

        System.out.println("varName: " + varName + " original number of events:" + cnt + " -> filteredEventNum: " + updatedPointers.size());
        varPointers.put(varName, updatedPointers);
        return swf.serialize();
    }

    public ByteBuffer updatePointers3(String varName, long window, int headTailMarker, OptimizedSWF swf, Map<String, Boolean> previousOrNext,
                                      Map<String, Map<Long, Set<String>>> pairs, Map<String, List<EqualDependentPredicate>> dependentPredicateMap){
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;
        UpdatedMarkers updatedMarkers = new UpdatedMarkers(swf.getBucketNum());
        List<Integer> pointers = varPointers.get(varName);
        List<Integer> updatedPointers = new ArrayList<>(pointers.size());

        int cnt = 0;
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(swf.query(ts, window)){
                cnt++;
                // check all related dependent predicates
                // for example, varName: c, previousVariables: a, b
                // dependent predicates: a.A1 = c.A1 + 5 AND b.A2 * 2 = c.A2
                boolean satisfied = true;
                for(String preVarName : previousOrNext.keySet()){
                    List<EqualDependentPredicate> dps = dependentPredicateMap.get(preVarName);

                    Map<Long, Set<String>> pairSet = pairs.get(preVarName);
                    EqualDependentPredicate firstEDP = dps.get(0);
                    String attrName = firstEDP.getAttrName(varName);
                    String attrValue = schema.getColumnValue(attrName, record).toString();

                    // false: previous, true: next
                    boolean next = previousOrNext.get(preVarName);
                    if(next){

                        boolean curWindowContains = pairSet.containsKey(ts / window) && pairSet.get(ts / window).contains(attrValue);
                        boolean nextWindowContains = pairSet.containsKey(ts / window + 1) && pairSet.get(ts / window + 1).contains(attrValue);
                        if(!curWindowContains && !nextWindowContains) {
                            satisfied = false;
                            break;
                        }
                    }else{
                        boolean curWindowContains = pairSet.containsKey(ts / window) && pairSet.get(ts / window).contains(attrValue);
                        boolean prevWindowContains = pairSet.containsKey(ts / window - 1) && pairSet.get(ts / window - 1).contains(attrValue);
                        if(!curWindowContains && !prevWindowContains){
                            satisfied = false;
                            break;
                        }
                    }
                }
                // if satisfy window condition and join condition
                if(satisfied){
                    updatedPointers.add(pointer);
                    updateShrinkFilter(ts + leftOffset, ts + rightOffset, window, swf, updatedMarkers);
                }
            }
        }

        System.out.println("varName: " + varName + " original number of events:" + cnt + " -> filteredEventNum: " + updatedPointers.size());

        varPointers.put(varName, updatedPointers);
        return updatedMarkers.serialize();
    }

    public ByteBuffer updatePointers4(String varName, long window, int headTailMarker, ReplayIntervals replayIntervals, Map<String, Boolean> previousOrNext,
                                      Map<String, BasicBF> bfMap, Map<String, List<EqualDependentPredicate>> dependentPredicateMap) {
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;
        List<Integer> pointers = varPointers.get(varName);

        List<Integer> updatedPointers = new ArrayList<>(pointers.size());
        ReplayIntervals updatedInterval = new ReplayIntervals();
        int cnt = 0;
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);

            if(replayIntervals.contains(ts)){
                cnt++;
                // check all related dependent predicates
                // for example, varName: c, previousVariables: a, b
                // dependent predicates: a.A1 = c.A1 + 5 AND b.A2 * 2 = c.A2
                boolean satisfied = true;
                for(String preVarName : previousOrNext.keySet()){
                    List<EqualDependentPredicate> dps = dependentPredicateMap.get(preVarName);
                    BasicBF lbf = bfMap.get(preVarName);
                    EqualDependentPredicate firstEDP = dps.get(0);
                    String attrName = firstEDP.getAttrName(varName);
                    Object obj = schema.getColumnValue(attrName, record);
                    String key = obj.toString();

                    // false: previous, true: next
                    boolean next = previousOrNext.get(preVarName);
                    if(next){
                        if(!lbf.contain(key, ts / window) && !lbf.contain(key, ts / window + 1)){
                            satisfied = false;
                            break;
                        }
                    }else{
                        if(!lbf.contain(key, ts/ window) && !lbf.contain(key, ts/ window - 1)){
                            satisfied = false;
                            break;
                        }
                    }
                }

                // if satisfy window condition and join condition
                if(satisfied){
                    updatedPointers.add(pointer);
                    updatedInterval.insertInOrder(ts + leftOffset, ts + rightOffset);
                }
            }
        }

        System.out.println("varName: " + varName + " original number of events:" + cnt + " -> filteredEventNum: " + updatedPointers.size());

        varPointers.put(varName, updatedPointers);
        return updatedInterval.serialize();
    }


    public ByteBuffer updatePointers(String varName, long window, int headTailMarker, ReplayIntervals replayIntervals, Map<String, Boolean> previousOrNext,
                                     Map<String, Map<Long, Set<String>>> pairs, Map<String, List<EqualDependentPredicate>> dependentPredicateMap){
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;
        List<Integer> pointers = varPointers.get(varName);
        List<Integer> updatedPointers = new ArrayList<>(pointers.size());

        ReplayIntervals updatedInterval = new ReplayIntervals();
        int cnt = 0;
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(replayIntervals.contains(ts)){
                cnt++;
                // check all related dependent predicates
                // for example, varName: c, previousVariables: a, b
                // dependent predicates: a.A1 = c.A1 + 5 AND b.A2 * 2 = c.A2
                boolean satisfied = true;
                for(String preVarName : previousOrNext.keySet()){
                    List<EqualDependentPredicate> dps = dependentPredicateMap.get(preVarName);

                    Map<Long, Set<String>> pairSet = pairs.get(preVarName);
                    EqualDependentPredicate firstEDP = dps.get(0);
                    String attrName = firstEDP.getAttrName(varName);
                    String attrValue = schema.getColumnValue(attrName, record).toString();

                    // false: previous, true: next
                    boolean next = previousOrNext.get(preVarName);
                    if(next){

                        boolean curWindowContains = pairSet.containsKey(ts / window) && pairSet.get(ts / window).contains(attrValue);
                        boolean nextWindowContains = pairSet.containsKey(ts / window + 1) && pairSet.get(ts / window + 1).contains(attrValue);
                        if(!curWindowContains && !nextWindowContains) {
                            satisfied = false;
                            break;
                        }
                    }else{
                        boolean curWindowContains = pairSet.containsKey(ts / window) && pairSet.get(ts / window).contains(attrValue);
                        boolean prevWindowContains = pairSet.containsKey(ts / window - 1) && pairSet.get(ts / window - 1).contains(attrValue);
                        if(!curWindowContains && !prevWindowContains){
                            satisfied = false;
                            break;
                        }
                    }
                }
                // if satisfy window condition and join condition
                if(satisfied){
                    updatedPointers.add(pointer);
                    updatedInterval.insertInOrder(ts + leftOffset, ts + rightOffset);
                }
            }
        }

        System.out.println("varName: " + varName + " original number of events:" + cnt + " -> filteredEventNum: " + updatedPointers.size());
        varPointers.put(varName, updatedPointers);
        updatedInterval.intersect(replayIntervals);
        System.out.println("after intersection, interval num: " + updatedInterval.getIntervals().size());
        return updatedInterval.serialize();
    }

    // return interval set, used for multiple
    public ByteBuffer updatePointers(String varName, long window, int headTailMarker, ReplayIntervals intervals){
        // headTailMarker only has three cases: 0 (leftmost), 1  (rightmost), 2 (middle)
        long leftOffset = (headTailMarker == 0) ? 0 : -window;
        long rightOffset = (headTailMarker == 1) ? 0 : window;

        intervals.sortAndReconstruct();
        ReplayIntervals newIntervals = new ReplayIntervals();

        List<Integer> pointers = varPointers.get(varName);
        List<Integer> updatedPointers = new ArrayList<>(pointers.size());
        // System.out.println("before filtering, key number: " + pointers.size());
        for(int pointer : pointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            // if events are ordered, we can use prefetchContains function to obtain better performance
            if(intervals.contains(ts)){
                newIntervals.insert(ts + leftOffset, ts + rightOffset);
                updatedPointers.add(pointer);
            }
        }
        // System.out.println("after filtering, key number:" + updatedPointers.size());
        varPointers.put(varName, updatedPointers);
        intervals.intersect(newIntervals);
        return intervals.serialize();
    }

    public List<byte[]> getRecords(ReplayIntervals intervals){
        List<Integer> mergedPointers = getMergedPointers();
        List<byte[]> ans = new ArrayList<>(2048);

        for(int pointer : mergedPointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(intervals.contains(ts)){
                ans.add(record);
            }
        }

        return ans;
    }

    public List<byte[]> getRecords(long window, OptimizedSWF swf){
        List<Integer> mergedPointers = getMergedPointers();
        int size = mergedPointers.size();
        List<byte[]> ans = new ArrayList<>(size >> 3);  // you can change this size

        for(int pointer : mergedPointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(swf.query(ts, window)){
                ans.add(record);
            }
        }
        return ans;
    }

    public List<byte[]> getRecords(long window, SWF swf){
        List<Integer> mergedPointers = getMergedPointers();
        int size = mergedPointers.size();
        List<byte[]> ans = new ArrayList<>(size >> 3);  // you can change this size

        for(int pointer : mergedPointers){
            byte[] record = records.get(pointer);
            long ts = schema.getTimestamp(record);
            if(swf.query(ts, window)){
                ans.add(record);
            }
        }
        return ans;
    }

    // merge two lists (if there having two same values, we only keep one value)
    private List<Integer> merge(List<Integer> list1, List<Integer> list2){
        if(list1 == null || list1.isEmpty()){
            return list2;
        }

        int size1 = list1.size();
        int size2 = list2.size();
        List<Integer> mergedList = new ArrayList<>(size1 + size2);

        int i = 0, j = 0;
        while(i < size1 && j < size2){
            if(list1.get(i) < list2.get(j)){
                mergedList.add(list1.get(i));
                i++;
            }else if (list1.get(i) > list2.get(j)){
                mergedList.add(list2.get(j));
                j++;
            }else{
                // if there having same two values we only keep one value
                mergedList.add(list1.get(i));
                i++;
                j++;
            }
        }

        while(i < size1){
            mergedList.add(list1.get(i));
            i++;
        }

        while(j < size2){
            mergedList.add(list2.get(j));
            j++;
        }
        return mergedList;
    }

    public void print(){
        for(Map.Entry<String, List<Integer>> entry : varPointers.entrySet()){
            String varName = entry.getKey();
            List<Integer> pointers = entry.getValue();
            System.out.println("varName: " + varName + " -> events:");
            for(Integer pointer : pointers){
                System.out.println("\t" + schema.getRecordStr(records.get(pointer)));
            }
        }
    }

    public void display(){
        for(String key : varPointers.keySet()){
            List<Integer> pointers = varPointers.get(key);
            int size = pointers.size();
            System.out.println("key: " + key + ", pointers size: " + size);
            for(int i = 1; i <= 10; i++){
                int idx = i * size / 10;
                System.out.println(schema.getRecordStr(records.get(idx)));
            }
        }
    }
}
