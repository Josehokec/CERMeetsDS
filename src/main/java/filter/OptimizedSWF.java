package filter;

import hasher.QuickHash;

import java.nio.ByteBuffer;

/**
 * OptimizedSWF: optimized shrinking window filter
 * it used to store time range intervals approximately
 * Besides, to reduce the network transmission overhead
 * we split [fingerprint, interval_marker], [hit_maker] (denoted as UpdatedMarkers)
 */
public class OptimizedSWF {
    // note that original paper MAX_KICK_OUT = 500, due to we swap choose 4 position to kick,
    // to achieve a high load factor, we enlarge MAX_KICK_OUT to 550
    static final int MAX_KICK_OUT = 550;
    private static final double LOAD_FACTOR = 0.955;
    private SFTable12_20 table;
    private int bucketNum;

    // ptr: 0, 1, 2, 3, ..., 19
    private static final int[] leftIntervalMarkerArray = {
            0xfffff, 0xffffe, 0xffffc, 0xffff8, 0xffff0, 0xfffe0, 0xfffc0, 0xfff80,
            0xfff00, 0xffe00, 0xffc00, 0xff800, 0xff000, 0xfe000, 0xfc000, 0xf8000,
            0xf0000, 0xe0000, 0xc0000, 0x80000,
    };

    // ptr: 0, 1, 2, 3, ..., 19
    private static final int[] rightIntervalMarkerArray = {
            0x00001, 0x00003, 0x00007, 0x0000f, 0x0001f, 0x0003f, 0x0007f, 0x000ff,
            0x001ff, 0x003ff, 0x007ff, 0x00fff, 0x01fff, 0x03fff, 0x07fff, 0x0ffff,
            0x1ffff, 0x3ffff, 0x7ffff, 0xfffff,
    };

    // ptr: 0, 1, 2, 3, ..., 19
    private static final int[] tags = {
            0x00001, 0x00002, 0x00004, 0x00008, 0x00010, 0x00020, 0x00040, 0x00080,
            0x00100, 0x00200, 0x00400, 0x00800, 0x01000, 0x02000, 0x04000, 0x08000,
            0x10000, 0x20000, 0x40000, 0x80000
    };

    private OptimizedSWF(SFTable12_20 table, int bucketNum) {
        this.table = table;
        this.bucketNum = bucketNum;
    }

    public static class Builder {
        private final long maxKeys;
        private SFTable12_20 table;

        //private int bitConfig;
        public Builder(long maxKeys) {
            this.maxKeys = maxKeys;
            table = null;
        }

        public Builder(SFTable12_20 table) {
            maxKeys = -1;
            this.table = table;
        }

        public OptimizedSWF build() {
            if (table != null) {
                int bucketNum = table.getBucketNum();
                return new OptimizedSWF(table, bucketNum);
            } else {
                int bucketNum = CFUtils.getBucketsNeeded(maxKeys, LOAD_FACTOR);
                table = SFTable12_20.createTable(bucketNum);
                return new OptimizedSWF(table, bucketNum);
            }
        }
    }

    static class SFVictim {
        private int bucketIndex;
        private int altBucketIndex;
        private long tag;

        SFVictim(int bucketIndex, int altBucketIndex, long tag) {
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

    public int getBucketNum() {
        return bucketNum;
    }

    // we have encapsulated the insertion function
    private boolean insert(long windowId, int marker){
        // if change length of fingerprint and markers, please modify below three lines
        int fpLen = 12;
        int rightShift = 32;
        int markerLen = 20;
        long hashCode = QuickHash.hash64(windowId);
        int bucketIndex = (int) ((hashCode >> rightShift) & (bucketNum - 1));
        long fp = hashCode >>> (64 - fpLen);
        int altBucketIndex = altIndex(bucketIndex, fp);
        long tag = (fp << markerLen) | (marker);

        //if we find same fingerprint, 2-nd bit is 0, otherwise 2-nd bit is 1
        int position = table.findFingerprint(bucketIndex, altBucketIndex, fp);
        // we need change
        if (position == 0x3) {
            return put(bucketIndex, altBucketIndex, tag);
        } else {
            // if we can find same fingerprint, then update
            return putStar(position >>> 2, position & 0x1, tag);
        }
    }

    /**
     * before inserting a time interval, we first find whether tag has exists,
     * if yes, then we directly modify markers; otherwise, we insert it into a vacant slot
     * @param startTs - start timestamp
     * @param endTs - end timestamp
     * @param window - query window
     * @return - true if insert successfully
     */
    public boolean insert(long startTs, long endTs, long window) {
        // new version: improve insertion throughput
        long startWindowId = startTs / window;
        long endWindowId = endTs / window;
        if(startWindowId == endWindowId){
            int marker = getLeftIntervalMarker(startTs, startWindowId * window, window) &
                    getRightIntervalMarker(endTs, endWindowId * window, window);
            return insert(startWindowId, marker);
        }else{
            boolean allInserted = true;
            int leftMostMarker = getLeftIntervalMarker(startTs, startWindowId * window, window);
            allInserted &= insert(startWindowId, leftMostMarker);
            int rightMostMarker = getRightIntervalMarker(endTs, endWindowId * window, window);
            allInserted &= insert(endWindowId, rightMostMarker);
            for(long wid = startWindowId + 1; wid < endWindowId; wid++){
                // because length of marker is 20 bits
                allInserted &= insert(wid, 0xfffff);
            }
            return allInserted;
        }
    }

    private int altIndex(int bucketIndex, long fingerprint) {
        long altIndex = bucketIndex ^ ((fingerprint + 1) * 0xc4ceb9fe1a85ec53L);
        // now pull into valid range
        return (int) (altIndex & (bucketNum - 1));
    }

    public static int getLeftIntervalMarker(long startTs, long offset, long window) {
        long distance = startTs - offset;
        // due to we default use 16 bits as interval marker and hit marker to quickly decode
        int ptr = (int) (distance * 20.0 / window);
        return leftIntervalMarkerArray[ptr];
    }

    public static int getRightIntervalMarker(long endTs, long offset, long window) {
        long distance = endTs - offset;
        int ptr = (int) (distance * 20.0 / window);
        return rightIntervalMarkerArray[ptr];
    }

    private boolean put(int bucketIndex, int altBucketIndex, long tag) {
        if (table.insertToBucket(bucketIndex, tag) || table.insertToBucket(altBucketIndex, tag)) {
            return true;
        }
        // kick operation
        int kickNum = 0;
        SFVictim victim = new SFVictim(bucketIndex, altBucketIndex, tag);
        while (kickNum < MAX_KICK_OUT) {
            if (randomSwap(victim)) {
                break;
            }
            kickNum++;
        }
        return kickNum != MAX_KICK_OUT;
    }

    // if we have find same fingerprint, then we directly insert
    private boolean putStar(int arrayPos, int highOrLow, long tag) {
        return table.insertToBucket(arrayPos, highOrLow, tag);
    }

    private boolean randomSwap(SFVictim victim) {
        // always choose alt bucket index
        int bucketIndex = victim.getAltBucketIndex();
        long replacedTag = table.swapRandomTagInBucket(bucketIndex, victim.getTag());
        int altBucketIndex = altIndex(bucketIndex, replacedTag >> 20);

        if (table.insertToBucket(altBucketIndex, replacedTag)) {
            return true;
        } else {
            // update victim
            victim.setBucketIndex(bucketIndex);
            victim.setAltBucketIndex(altBucketIndex);
            victim.setTag(replacedTag);
            return false;
        }
    }

    public boolean query(long ts, long window) {
        // if change length of fingerprint and markers, please modify below four lines
        int fpLen = 12;
        int rightShift = 32;
        int markerLen = 20;
        double doubleMarkerLen = 20.0;

        long windowId = ts / window;
        long hashCode = QuickHash.hash64(windowId);
        long fp = hashCode >>> (64 - fpLen);
        int bucketIndex = (int) ((hashCode >> rightShift) & (bucketNum - 1));
        int altBucketIndex = altIndex(bucketIndex, fp);
        // interval_marker occupies 20 bits
        long distance = ts - windowId * window;
        int ptr = (int) (distance * doubleMarkerLen / window);
        long tag = tags[ptr] | (fp << markerLen);
        return table.findTag(bucketIndex, altBucketIndex, tag);
    }

    /**
     * faster code lines
     * @param windowId window id
     * @return composed key: bucketIdx (high 29 bits), slotIdx (low 3 bits)
     */
    public int queryWindowId(long windowId){
        // if change length of fingerprint and markers, please modify below two lines
        int fpLen = 12;
        int rightShift = 32;
        long hashCode = QuickHash.hash64(windowId);
        long fp = hashCode >>> (64 - fpLen);
        // use high 32 bits as bucket index
        int bucketIndex = (int) ((hashCode >> rightShift) & (bucketNum - 1));
        int altBucketIndex = altIndex(bucketIndex, fp);
        return table.findFingerprint(bucketIndex, altBucketIndex, fp);
    }

    public int getKeyNum() {
        return table.getKeyNum();
    }

    public long getSliceNum(){
        return table.getSliceNum();
    }

    // we need to improve this
    public void rebuild(UpdatedMarkers updatedMarkers) {
        int keyNum = table.updateMarkers(updatedMarkers);

        // old version: bucketNum * 4.0 * ShrinkFilterUltra.LOAD_FACTOR
        // we choose 0.78 because we find 0.78 can keep a low update overhead
        double maxKeyNum = bucketNum * 4 * 0.78;
        int compactCnt = 0;
        while(keyNum < maxKeyNum * 0.5 && bucketNum >= 16){
            compactCnt++;
            maxKeyNum /= 2;
        }
        //System.out.println("shrink filter ultra have reduced the space by " + compactCnt + " times");

        if(compactCnt == 0){
            return;
        }

        // to avoid compact fail, we need to save a snapshot
        // SFTable12_20 copiedTable = table.copy();
        bucketNum = bucketNum >> compactCnt;
        int arrayLen = bucketNum << 1;
        long[] semiTable = table.getSemiTable(compactCnt);

        int tableLen = semiTable.length;
        for(int i = 0; i < tableLen; i++){
            long value = semiTable[i];
            int bucketIdx = (i % arrayLen) >> 1;
            for(int j = 0; j < 2; j++){
                long slot = value & 0xffffffffL;
                long fp = slot >> 20;
                int altBucketIdx = altIndex(bucketIdx, fp);
                int position = table.findFingerprint(bucketIdx, altBucketIdx, fp);
                if (position == 0x3) {
                    // due to we set a low load factor (0.78), thus, it almost did not trigger the rollback operation
                    put(bucketIdx, altBucketIdx, slot);
//                    if (!put(bucketIdx, altBucketIdx, slot)) {
//                        // if we find sfu cannot compact, we need to rollback
//                        System.out.println("rollback");
//                        table = copiedTable;
//                        bucketNum = bucketNum << 1;
//                        return;
//                    }
                } else {
                    // due to we set a low load factor (0.78), thus, it almost did not trigger the rollback operation
                    putStar(position >>> 2, position & 0x1, slot);
//                    if (!putStar(position >>> 2, position & 0x1, slot)) {
//                        // if we find sfu cannot compact, we need to rollback
//                        System.out.println("rollback");
//                        table = copiedTable;
//                        bucketNum = bucketNum << 1;
//                        return;
//                    }
                }
                value >>>= 32;
            }
        }
    }

    public void display(){
        table.displayWithHex();
    }

    public ByteBuffer serialize(){
        return table.serialize();
    }

    public static OptimizedSWF deserialize(ByteBuffer buffer) {
        // ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int arrayLen = buffer.getInt();
        long[] bitSet = new long[arrayLen];
        // we cannot call this function: buffer.get(byte[] ...)
        for (int i = 0; i < arrayLen; ++i) {
            long bucketContent = buffer.getLong();
            bitSet[i] = bucketContent;
        }
        return new OptimizedSWF.Builder(SFTable12_20.createTable(bitSet)).build();
    }
}
