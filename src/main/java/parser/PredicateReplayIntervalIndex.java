package parser;

import utils.ReplayIntervals;

import java.util.*;

public class PredicateReplayIntervalIndex {

    public enum EvictionStrategy {
        LRU,
        LARGEST_SIZE
    }

    public static long MAX_SINGLE_SIZE = 1024 * 1024;      // 1 MB
    public static long MAX_SIZE_BYTES = 20 * 1024 * 1024;  // default 20 MB
    public static EvictionStrategy EVICTION_STRATEGY = EvictionStrategy.LRU;

    private static class NormalizedPredicate {
        String attribute;
        ComparedOperator operator;
        String constantRaw;
        boolean numeric;
        double constantNum;

        NormalizedPredicate(IndependentPredicate p) {
            String attr = p.getAttributeName();
            int dotIdx = attr.indexOf('.');
            this.attribute = dotIdx != -1 ? attr.substring(dotIdx + 1) : attr;
            this.operator = p.getOperator();
            this.constantRaw = p.getConstantValue();

            Double num = tryParseNumber(this.constantRaw);
            if (num != null) {
                this.numeric = true;
                this.constantNum = num;
            } else {
                this.numeric = false;
            }
        }

        private static Double tryParseNumber(String s) {
            try {
                return Double.parseDouble(s);
            } catch (Exception ignore) {
                return null;
            }
        }
    }

    private static class ConditionEntry {
        List<NormalizedPredicate> predicates;
        byte[] compressedIntervals;
        long lastAccessTime;
        long cachedSize = -1;
        long intervalLen = -1;

        ConditionEntry(List<NormalizedPredicate> predicates, ReplayIntervals intervals) {
            this.predicates = predicates;
            intervalLen = intervals.getTimeLength();
            this.compressedIntervals = intervals.serializeCompressed().array();
            this.lastAccessTime = System.nanoTime();
        }

        public ReplayIntervals getIntervals() {
            return ReplayIntervals.deserializeCompressed(java.nio.ByteBuffer.wrap(compressedIntervals));
        }

        public long getSize() {
            if (cachedSize != -1) return cachedSize;
            long size = 24; // len(lastAccessTime) + len(intervalLen) + len(cachedSize)
            for (NormalizedPredicate predicate : predicates) {
                size += predicate.constantRaw.length();
            }
            cachedSize = size + compressedIntervals.length;
            return cachedSize;
        }

        public long getIntervalLength() {
            return intervalLen;
        }
    }

    private static class Interval {
        double min = Double.NEGATIVE_INFINITY;
        double max = Double.POSITIVE_INFINITY;
        static Interval from(List<NormalizedPredicate> ps, String attr) {
            Interval iv = new Interval();
            for (NormalizedPredicate p : ps) {
                if (p.attribute.equals(attr) && p.numeric) {
                    switch (p.operator) {
                        case LT: case LE: iv.max = Math.min(iv.max, p.constantNum); break;
                        case GT: case GE: iv.min = Math.max(iv.min, p.constantNum); break;
                        case EQ: iv.min = Math.max(iv.min, p.constantNum); iv.max = Math.min(iv.max, p.constantNum); break;
                        default: break;
                    }
                }
            }
            return iv;
        }
    }

    private final List<ConditionEntry> entries = new ArrayList<>();
    private final Map<Double, List<ConditionEntry>> mBuckets = new HashMap<>();
    private final HashMap<String, List<ConditionEntry>> attrBuckets = new HashMap<>(); // fallback
    private long currentSize = 0;

    public void display(){
        System.out.println("-----Current entries in index:------");
        for (ConditionEntry entry : entries) {
            System.out.print("Predicates: ");
            for (NormalizedPredicate np : entry.predicates) {
                System.out.print(np.attribute + " " + np.operator + " " + np.constantRaw + "; ");
            }
            System.out.println();
            // System.out.println(" | Intervals: " + entry.getIntervals().getIntervals());
        }
    }

    public long getSize() {
        return currentSize;
    }

    private void evict() {
        if (entries.isEmpty()) return;

        ConditionEntry victim = null;
        if (EVICTION_STRATEGY == EvictionStrategy.LRU) {
            victim = entries.get(0);
            for (ConditionEntry e : entries) {
                if (e.lastAccessTime < victim.lastAccessTime) {
                    victim = e;
                }
            }
        } else if (EVICTION_STRATEGY == EvictionStrategy.LARGEST_SIZE) {
            victim = entries.get(0);
            long maxLen = victim.getIntervalLength();
            for (ConditionEntry e : entries) {
                long len = e.getIntervalLength();
                if (len > maxLen) {
                    maxLen = len;
                    victim = e;
                }
            }
        }

        if (victim != null) {
            entries.remove(victim);
            currentSize -= victim.getSize();

            Double mVal = null;
            for (NormalizedPredicate np : victim.predicates) {
                if ("m".equals(np.attribute) && np.operator == ComparedOperator.EQ) {
                    mVal = np.constantNum;
                    break;
                }
            }

            if (mVal != null) {
                List<ConditionEntry> bucket = mBuckets.get(mVal);
                if (bucket != null) {
                    bucket.remove(victim);
                    if (bucket.isEmpty()) {
                        mBuckets.remove(mVal);
                    }
                }
            } else {
                for (NormalizedPredicate np : victim.predicates) {
                    List<ConditionEntry> bucket = attrBuckets.get(np.attribute);
                    if (bucket != null) {
                        bucket.remove(victim);
                        if (bucket.isEmpty()) {
                            attrBuckets.remove(np.attribute);
                        }
                    }
                }
            }
        }
    }

    public void putCondition(List<IndependentPredicate> predicates, ReplayIntervals intervals) {
        List<NormalizedPredicate> nps = normalize(predicates);
        ConditionEntry entry = new ConditionEntry(nps, intervals);

        long newSize = entry.getSize();

        if(newSize > MAX_SINGLE_SIZE){
            return;
        }

        while (currentSize + newSize > MAX_SIZE_BYTES && !entries.isEmpty()) {
            evict();
        }

        if (currentSize + newSize > MAX_SIZE_BYTES) {
            return;
        }

        entries.add(entry);
        currentSize += newSize;

        Double mVal = null;
        for (NormalizedPredicate np : nps) {
            if ("m".equals(np.attribute) && np.operator == ComparedOperator.EQ) {
                mVal = np.constantNum;
                break;
            }
        }

        if (mVal != null) {
            mBuckets.computeIfAbsent(mVal, k -> new ArrayList<>()).add(entry);
        } else {
            for (NormalizedPredicate np : nps) {
                attrBuckets.computeIfAbsent(np.attribute, k -> new ArrayList<>()).add(entry);
            }
        }
    }

    public abstract static class QueryResult {
        public ReplayIntervals intervals;
        public int exactMatch; // 1 for perfect match, 0 otherwise

        public QueryResult(ReplayIntervals intervals, int exactMatch) {
            this.intervals = intervals;
            this.exactMatch = exactMatch;
        }

        public String toString() {
            return "QueryResult{intervals=" + intervals.getIntervals() + ", exactMatch=" + exactMatch + "}";
        }
    }

    public QueryResult query(List<IndependentPredicate> queryPredicates) {
        List<NormalizedPredicate> qs = normalize(queryPredicates);
        if (qs.isEmpty()) {
            return null;
        }

        Double queryMVal = null;
        for (NormalizedPredicate q : qs) {
            if ("m".equals(q.attribute) && q.operator == ComparedOperator.EQ) {
                queryMVal = q.constantNum;
                break;
            }
        }

        List<ConditionEntry> candidates;
        if (queryMVal != null && mBuckets.containsKey(queryMVal)) {
            candidates = mBuckets.get(queryMVal);
        } else {
            candidates = entries;
        }

        List<ConditionEntry> validEntries = new ArrayList<>();
        Set<NormalizedPredicate> exactlyCoveredQs = new HashSet<>();

        for (ConditionEntry entry : candidates) {
            boolean isValid = true;
            Set<NormalizedPredicate> exactlyCoveredByThis = new HashSet<>();

            for (NormalizedPredicate s : entry.predicates) {
                boolean covered = false;
                for (NormalizedPredicate q : qs) {
                    if (s.attribute.equals(q.attribute) && predicateCovers(s, q)) {
                        covered = true;
                        if (isExactMatch(s, q)) {
                            exactlyCoveredByThis.add(q);
                        }
                        break;
                    }
                }
                if (!covered) {
                    isValid = false;
                    break;
                }
            }

            if (isValid) {
                validEntries.add(entry);
                exactlyCoveredQs.addAll(exactlyCoveredByThis);
            }
        }

        if (validEntries.isEmpty()) {
            return null;
        }

        ReplayIntervals result = null;
        for (ConditionEntry entry : validEntries) {
            entry.lastAccessTime = System.nanoTime();
            if (result == null) {
                result = entry.getIntervals();
            } else {
                result.intersect(entry.getIntervals());
            }
        }

        if (result == null || result.getIntervals().isEmpty()) {
            return null;
        }

        int exactMatch = exactlyCoveredQs.size() >= qs.size() ? 1 : 0;
        return new QueryResult(result, exactMatch) {};
    }

    private ConditionEntry findBestEntryForSingleQueryPredicate(NormalizedPredicate q, int queryPredicateCount, Double queryMVal) {
        List<ConditionEntry> bucket;
        if (queryMVal != null && mBuckets.containsKey(queryMVal)) {
            bucket = mBuckets.get(queryMVal);
        } else {
            bucket = attrBuckets.get(q.attribute);
        }

        if (bucket == null || bucket.isEmpty()) {
            return null;
        }

        ConditionEntry bestEntry = null;
        NormalizedPredicate bestCoverPred = null;

        for (ConditionEntry entry : bucket) {
            if (queryPredicateCount == 1 && entry.predicates.size() != 1) {
                continue;
            }

            for (NormalizedPredicate s : entry.predicates) {
                if (!s.attribute.equals(q.attribute)) {
                    continue;
                }
                if (!predicateCovers(s, q)) {
                    continue;
                }

                if (bestEntry == null) {
                    bestEntry = entry;
                    bestCoverPred = s;
                } else if (isBetterCoverForSinglePredicate(s, bestCoverPred, q)) {
                    bestEntry = entry;
                    bestCoverPred = s;
                }
                break;
            }
        }

        return bestEntry;
    }

    private boolean isBetterCoverForSinglePredicate(
            NormalizedPredicate candidate,
            NormalizedPredicate currentBest,
            NormalizedPredicate q
    ) {
        if (!candidate.numeric || !currentBest.numeric || !q.numeric) {
            return false;
        }

        if (candidate.operator != currentBest.operator) {
            return false;
        }

        switch (q.operator) {
            case LT:
            case LE:
                return candidate.constantNum < currentBest.constantNum;
            case GT:
            case GE:
                return candidate.constantNum > currentBest.constantNum;
            case EQ:
            case NEQ:
            default:
                return false;
        }
    }

    private static List<NormalizedPredicate> normalize(List<IndependentPredicate> predicates) {
        List<NormalizedPredicate> list = new ArrayList<>(predicates.size());
        for (IndependentPredicate p : predicates) {
            list.add(new NormalizedPredicate(p));
        }
        return list;
    }

    private static boolean predicateCovers(NormalizedPredicate s, NormalizedPredicate q) {
        if (s.numeric != q.numeric) {
            return false;
        }

        if (!s.numeric) {
            return s.operator == q.operator && s.constantRaw.equals(q.constantRaw);
        }

        double sv = s.constantNum;
        double qv = q.constantNum;

        switch (s.operator) {
            case LT: return coverByLt(sv, q.operator, qv);
            case LE: return coverByLe(sv, q.operator, qv);
            case GT: return coverByGt(sv, q.operator, qv);
            case GE: return coverByGe(sv, q.operator, qv);
            case EQ: return coverByEq(sv, q.operator, qv);
            case NEQ: return coverByNeq(sv, q.operator, qv);
            default: return false;
        }
    }

    private static boolean coverByLt(double s, ComparedOperator qop, double q) {
        switch (qop) {
            case LT: return q <= s;
            case LE: return q < s;
            case EQ: return q < s;
            default: return false;
        }
    }

    private static boolean coverByLe(double s, ComparedOperator qop, double q) {
        switch (qop) {
            case LT: return q <= s;
            case LE: return q <= s;
            case EQ: return q <= s;
            default: return false;
        }
    }

    private static boolean coverByGt(double s, ComparedOperator qop, double q) {
        switch (qop) {
            case GT: return q >= s;
            case GE: return q > s;
            case EQ: return q > s;
            default: return false;
        }
    }

    private static boolean coverByGe(double s, ComparedOperator qop, double q) {
        switch (qop) {
            case GT: return q >= s;
            case GE: return q >= s;
            case EQ: return q >= s;
            default: return false;
        }
    }

    private static boolean coverByEq(double s, ComparedOperator qop, double q) {
        return qop == ComparedOperator.EQ && Double.compare(s, q) == 0;
    }

    private static boolean coverByNeq(double s, ComparedOperator qop, double q) {
        if (qop == ComparedOperator.EQ) {
            return Double.compare(q, s) != 0;
        }
        return false;
    }

    private static boolean isExactMatch(NormalizedPredicate s, NormalizedPredicate q) {
        if (s.numeric != q.numeric) return false;
        if (s.operator != q.operator) return false;
        if (!s.numeric) return s.constantRaw.equals(q.constantRaw);
        return Double.compare(s.constantNum, q.constantNum) == 0;
    }

    // ---- test demo ----
    public static void main(String[] args) {
        PredicateReplayIntervalIndex index = new PredicateReplayIntervalIndex();
        ReplayIntervals intervals1 = new ReplayIntervals();
        intervals1.insert(1, 14);intervals1.insert(18, 28);// A.price<11 ==> {1, 4, 18}

        ReplayIntervals intervals2 = new ReplayIntervals();
        intervals2.insert(4, 14);intervals2.insert(18, 33); // A.price>8 ==> {4, 18, 23}

        ReplayIntervals intervals3 = new ReplayIntervals();
        intervals3.insert(1, 15);intervals3.insert(18, 39); // A.vol<=100 ==> {1, 4, 5, 18, 24, 29}

        ReplayIntervals intervals4 = new ReplayIntervals();
        intervals4.insert(4, 14);intervals4.insert(18, 34); // A.price<15 AND A.vol<=120 ==> {4, 18, 24}

        ReplayIntervals intervals5 = new ReplayIntervals();
        intervals5.insert(3, 13);intervals5.insert(18, 28);intervals5.insert(33, 43); // A.type='book' ==> {3, 18, 33}


        // 0: head variable, 1: tail variable, 2: middle variable
        // x.w ==> window condition
        // x.m ==> head/tail/middle variable condition
        index.putCondition(list("A.price<11", "x.w<=10", "x.m=0"), intervals1);
        index.putCondition(list("A.price>8", "x.w<=10", "x.m=0"), intervals2);
        index.putCondition(list("A.vol<=100", "x.w<=10", "x.m=0"), intervals3);
        index.putCondition(list("A.price<15", "A.vol<=120", "x.w<=10", "x.m=0"), intervals4);
        index.putCondition(list("A.type='book'", "x.w<=10", "x.m=0"), intervals5);

        // QueryResult{intervals=[[1,14], [18,28]], exactMatch=0}
        System.out.println("query: B.price<8 AND x.w<=10 AND x.m=0, result:" + index.query(list("B.price<8", "x.w<=10", "x.m=0")));
        // QueryResult{intervals=[[1,14], [18,28]], exactMatch=0}
        System.out.println("query: B.price<11 AND x.w<=5 AND x.m=0, result:" + index.query(list("B.price<11", "x.w<=5", "x.m=0")));
        // QueryResult{intervals=[[1,14], [18,28]], exactMatch=1}
        System.out.println("query: B.price<11 AND x.w<=10 AND x.m=0, result:" + index.query(list("B.price<11", "x.w<=10", "x.m=0")));
        // QueryResult{intervals=[[4,14], [18,28]], exactMatch=1}
        System.out.println("query: B.price<11 AND B.price>8 AND x.w<=10 AND x.m=0, result:" + index.query(list("B.price<11", "B.price>8", "x.w<=10", "x.m=0")));
        // QueryResult{intervals=[[1,15], [18,39]], exactMatch=1}
        System.out.println("query: B.vol<100 AND AND x.w<=10 AND x.m=0, result:" + index.query(list("B.vol<=100", "x.w<=10", "x.m=0")));
        // QueryResult{intervals=[[1,14], [18,28]], exactMatch=0}
        System.out.println("query: B.vol<100 AND A.price<10 AND x.w<=10 AND x.m=0, result:" + index.query(list("B.vol<100", "A.price<10", "x.w<=10", "x.m=0")));
        // QueryResult{intervals=[[1,14], [18,28]], exactMatch=1}
        System.out.println("query: B.vol<=100 AND A.price<11 AND x.w<=10 AND x.m=0, result:" + index.query(list("B.vol<=100", "A.price<11", "x.w<=10", "x.m=0")));

        System.out.println("query: B.vol<=120 AND A.price<11 AND x.w<=10 AND x.m=0, result:" + index.query(list("B.vol<=120", "A.price<11", "x.w<=10", "x.m=0")));


        // QueryResult{intervals=[[1,14], [18,28]], exactMatch=0}
        System.out.println("query: B.type='apple' AND B.price<11 AND x.w<=10 AND x.m=0, result:" + index.query(list("B.type='apple'", "B.price<11", "x.w<=10", "x.m=0")));
        // QueryResult{intervals=[[3,13], [18,28], [33,43]], exactMatch=1}
        System.out.println("query: B.type='book' AND x.w<=10 AND x.m=0, result:" + index.query(list("B.type='book'", "x.w<=10", "x.m=0")));
    }

    private static List<IndependentPredicate> list(String... arr) {
        List<IndependentPredicate> ps = new ArrayList<>();
        for (String s : arr) {
            ps.add(new IndependentPredicate(s));
        }
        return ps;
    }
}