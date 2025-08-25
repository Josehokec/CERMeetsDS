package filter;

import compute.Args;

import java.nio.ByteBuffer;

/**
 * Shrink filter (SF) is used for test a timestamp whether within time interval set.
 * SF is based on Cuckoo filter (please see Cuckoo Filter: Practically Better Than Bloom. In CoNEXT, 2014.)
 * We choose filter rather than hashmap or replay intervals (see utils package) because
 * (1) filter has a lower space overhead and higher load factor,
 * (2) filter is merge-able, and it has O(1) insertion/query/update latency
 * Here is our default setting:
 * BUCKET_SIZE = 4, LOAD_FACTOR = 0.955, MAX_KICK_OUT = 550
 */
public class SWF {
    // note that original paper MAX_KICK_OUT = 500, due to we swap choose 4 position to kick,
    // to achieve a high load factor, we enlarge MAX_KICK_OUT to 550
    static final int MAX_KICK_OUT = 550;
    // change this value to modify shrink filter mode
    // we provide three modes: _8_12_12, _12_10_10, _16_8_8
    public static SFTableMode SHRINK_FILTER_TABLE_MODE = Args.sfTableMode;
    public static int allOneMarker = 0xffc00;
    private static final double LOAD_FACTOR = 0.955;

    private static final int[] allOneHitMarkers = {0xfff, 0x3ff, 0xff};
    private static final int[] leftShits = {24, 20, 16};

    private AbstractSFTable table;
    private int bucketNum;

    public int getBucketNum(){return bucketNum;}
    public long getSemiBucketValue(int pos){return table.getLongValue(pos);}

    private SWF(AbstractSFTable table, int bucketNum, SFTableMode mode){
        this.table = table;
        this.bucketNum = bucketNum;
        SHRINK_FILTER_TABLE_MODE = mode;

        switch (SHRINK_FILTER_TABLE_MODE){
            case _12_10_10:
                allOneMarker = 0xffc00;
                break;
            case _8_12_12:
                allOneMarker = 0xfff000;
                break;
            case _16_8_8:
                allOneMarker = 0xff00;
        }
    }

    public static class Builder {
        private final long maxKeys;
        private AbstractSFTable table;
        //private int bitConfig;
        public Builder(long maxKeys) {
            this.maxKeys = maxKeys;
            table = null;
        }

        public Builder(AbstractSFTable table) {
            maxKeys = -1;
            this.table = table;
        }

        public SWF build(){
            int bucketNum;
            if(table != null){
                bucketNum = table.getBucketNum();
            }else{
                bucketNum = CFUtils.getBucketsNeeded(maxKeys, LOAD_FACTOR);
                switch (SHRINK_FILTER_TABLE_MODE){
                    case _8_12_12:
                        table = SFTable8_12_12.create(bucketNum);
                        break;
                    case _12_10_10:
                        table = SFTable12_10_10.create(bucketNum);
                        break;
                    case _16_8_8:
                        table = SFTable16_8_8.create(bucketNum);
                        break;
                    default:
                        System.out.println("we cannot support this mode");
                        table = SFTable12_10_10.create(bucketNum);
                        break;
                }
            }
            return new SWF(table, bucketNum, SHRINK_FILTER_TABLE_MODE);
        }
    }

    static class SFVictim {
        private int bucketIndex;
        private int altBucketIndex;
        private long tag;

        //WCFVictim(){ tag = -1; }

        SFVictim(int bucketIndex, int altBucketIndex, long tag){
            this.bucketIndex = bucketIndex;
            this.altBucketIndex = altBucketIndex;
            this.tag = tag;
        }

        int getBucketIndex() {
            return bucketIndex;
        }

        void setBucketIndex(int bucketIndex) {
            this.bucketIndex = bucketIndex;
        }

        int getAltBucketIndex() {
            return altBucketIndex;
        }

        void setAltBucketIndex(int altBucketIndex) {
            this.altBucketIndex = altBucketIndex;
        }

        long getTag() {
            return tag;
        }

        void setTag(long tag) {
            this.tag = tag;
        }
    }

    public boolean insert(long windowId, int marker){
        // low 32 bits are bucketIndex, high 32 bits are fingerprint
        long fp_bucketIdx = table.generate(windowId);
        // we do not use 0xffffffffL;
        int bucketIdx = (int) (fp_bucketIdx & 0x3fff_ffff);
        long fp = fp_bucketIdx >>> 32;
        int altBucketIdx  = table.altIndex(bucketIdx, fp);
        long tag = (fp << leftShits[SHRINK_FILTER_TABLE_MODE.ordinal()]) | marker;

        // System.out.println("windowId:" + windowId + " tag: " + Long.toHexString(tag) + " bucketIdx: " + bucketIdx + " altBucketIdx: " + altBucketIdx);
        return put(bucketIdx, altBucketIdx, tag);
    }

    /**
     * insert a time interval
     * @param startTs - start timestamp
     * @param endTs - end timestamp
     * @param window - query window
     * @return - true if insert successfully
     */
    public boolean insert(long startTs, long endTs, long window){
        // new version: improve insertion throughput
        long startWindowId = startTs / window;
        long endWindowId = endTs / window;
        if(startWindowId == endWindowId){
            int marker = table.getLeftIntervalMarker(startTs, startWindowId * window, window) &
                    table.getRightIntervalMarker(endTs, endWindowId * window, window);
            return insert(startWindowId, marker);
        }else{
            boolean allInserted = true;
            int leftMostMarker = table.getLeftIntervalMarker(startTs, startWindowId * window, window);
            allInserted &= insert(startWindowId, leftMostMarker);
            int rightMostMarker = table.getRightIntervalMarker(endTs, endWindowId * window, window);
            allInserted &= insert(endWindowId, rightMostMarker);
            for(long wid = startWindowId + 1; wid < endWindowId; wid++){
                allInserted &= insert(wid, allOneMarker);
            }
            return allInserted;
        }
    }

    private boolean put(int bucketIndex, int altBucketIndex, long tag){
        //System.out.println("i1: " + bucketIndex + ", i2: " + altBucketIndex + ", tag: " + Long.toHexString(tag));
        if(table.insertToBucket(bucketIndex, tag) || table.insertToBucket(altBucketIndex, tag)){
            return true;
        }
        int kickNum = 0;
        SFVictim victim = new SFVictim(bucketIndex, altBucketIndex, tag);
        while(kickNum < MAX_KICK_OUT){
            if(randomSwap(victim)){
                break;
            }
            kickNum++;
        }

        return kickNum != MAX_KICK_OUT;
    }

    private boolean randomSwap(SFVictim victim){
        // always choose alt bucket index
        int bucketIndex = victim.getAltBucketIndex();
        long replacedTag = table.swapRandomTagInBucket(bucketIndex, victim.getTag());

        int altBucketIndex;
        switch(SHRINK_FILTER_TABLE_MODE){
            case _8_12_12:
                altBucketIndex = table.altIndex(bucketIndex, replacedTag >> 24);
                break;
            case _12_10_10:
                altBucketIndex = table.altIndex(bucketIndex, replacedTag >> 20);
                break;
            case _16_8_8:
                altBucketIndex = table.altIndex(bucketIndex, replacedTag >> 16);
                break;
            default:
                System.out.println("exception...");
                altBucketIndex = table.altIndex(bucketIndex, replacedTag >> 20);
        }
        if(table.insertToBucket(altBucketIndex, replacedTag)){
            return true;
        }else{
            // update victim
            victim.setBucketIndex(bucketIndex);
            victim.setAltBucketIndex(altBucketIndex);
            victim.setTag(replacedTag);
            return false;
        }
    }

    public boolean query(long ts, long window){
        // according to ts, find whether an interval window includes this ts
        long windowId = ts / window;
        long fp_bucketIdx = table.generate(windowId);
        long fingerprint = fp_bucketIdx >>> 32;
        long distance = ts - windowId * window;
        long tag = table.getTag(window, fingerprint, distance);
        int bucketIndex = (int) (fp_bucketIdx & 0x3fffffff);
        int altBucketIndex = table.altIndex(bucketIndex, fingerprint);
        //System.out.println("query tag: " + Long.toHexString(tag) + " | two buckets: " + bucketIndex + ", " + altBucketIndex);
        return table.findTag(bucketIndex, altBucketIndex, tag);
    }

    public void updateRange(int hitMarker, long fp_bucketIdx){
        long fingerprint = fp_bucketIdx >>> 32;
        int bucketIdx = (int) (fp_bucketIdx & 0x3fffffff);
        int altBucketIdx = table.altIndex(bucketIdx, fingerprint);

        //System.out.println("fp: " + Long.toHexString(fingerprint) + " bucketIdx: " + bucketIdx + " altBucketIdx: " + altBucketIdx);
        table.updateTagInBucket(bucketIdx, altBucketIdx, fingerprint, hitMarker);
    }

    public long getSliceNum(){
        return table.getSliceNum();
    }

    public void rebuild(){
        int compactCnt = table.rebuildTable();
        bucketNum >>= compactCnt;
    }

    public int getKeyNum(){
        return table.getKeyNum();
    }

    /**
     * when querying returns true, we need to update hit_markers
     * we need to ensure this key exists
     * @param startTs - start timestamp
     * @param endTs   - end timestamp
     * @param window  - query window
     */
    public void updateRange(long startTs, long endTs, long window){
        long startWindowId = startTs / window;
        long endWindowId = endTs / window;

        if(endWindowId == startWindowId){
            long fp_bucketIdx = table.generate(startWindowId);
            int hitMarker = table.getLeftHitMarker(startTs, startWindowId * window, window) &
                    table.getRightHitMarker(endTs, endWindowId * window, window);
            updateRange(hitMarker, fp_bucketIdx);
        }else{
            long leftMostFp_bucketIdx = table.generate(startWindowId);
            int leftMostHitMarker = table.getLeftHitMarker(startTs, startWindowId * window, window);
            updateRange(leftMostHitMarker, leftMostFp_bucketIdx);

            long rightMostFp_bucketIdx = table.generate(endWindowId);
            int rightMostHitMarker = table.getRightHitMarker(endTs, endWindowId * window, window);
            updateRange(rightMostHitMarker, rightMostFp_bucketIdx);

            for(long wid = startWindowId + 1; wid < endWindowId; wid++){
                long fp_bucketIdx = table.generate(wid);
                int hitMarker = allOneHitMarkers[SHRINK_FILTER_TABLE_MODE.ordinal()];
                updateRange(hitMarker, fp_bucketIdx);
            }
        }
    }

    public AbstractSFTable getTable(){
        return table;
    }

    public void or(SWF swf){
        table.merge(swf.getTable());
    }

    public ByteBuffer serialize(){
        return table.serialize();
    }

    public static SWF deserialize(ByteBuffer buffer){
        // ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int arrayLen = buffer.getInt();
        long[] bitSet = new long[arrayLen];
        for(int i = 0; i < arrayLen; ++i){
            long bucketContent = buffer.getLong();
            bitSet[i] = bucketContent;
        }
        switch (SHRINK_FILTER_TABLE_MODE){
            case _8_12_12:
                return new SWF.Builder(SFTable8_12_12.create(bitSet)).build();
            case _12_10_10:
                return new SWF.Builder(SFTable12_10_10.create(bitSet)).build();
            case _16_8_8:
                return new SWF.Builder(SFTable16_8_8.create(bitSet)).build();
            default:
                System.out.println("without this mode, we use SFTable12_10_10...");
                return new SWF.Builder(SFTable12_10_10.create(bitSet)).build();
        }
    }
}
