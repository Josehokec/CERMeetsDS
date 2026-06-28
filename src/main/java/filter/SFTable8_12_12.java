package filter;

import hasher.QuickHash;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Shrink Filter Table (SFT)
 * the tag (entry) of SFT is:
 * ---- fingerprint (8 bits) + interval_marker (12 bits) + hit_marker (12 bits)
 * bucket size of SFT is 4,which means a bucket of SFT occupies 128 bits
 * bucket storage format:    |entry1|entry0|
 *                           |entry3|entry2|
 */
public class SFTable8_12_12 extends AbstractSFTable {
    private long[] bitSet;

    // ptr: 0, 1, 2, 3, ..., 11
    // interval_marker, hit_marker: 0000 0000 0001 0000 0000 0000 -> 0x001000
    private final int[] tags = {
            0x001000, 0x002000, 0x004000, 0x008000, 0x010000, 0x020000,
            0x040000, 0x080000, 0x100000, 0x200000, 0x400000, 0x800000
    };

    // range: [1111 1111 1111 0000 0000 0000, 1000 0000 0000 0000 0000 0000]
    private static final int[] leftIntervalMarkers = {
            0xfff000, 0xffe000, 0xffc000, 0xff8000, 0xff0000, 0xfe0000, 0xfc0000, 0xf80000, 0xf00000, 0xe00000, 0xc00000, 0x800000
    };

    // range: [0000 0000 0001 0000 0000 0000, 1111 1111 1111 0000 0000 0000]
    private static final int[] rightIntervalMarkers = {
            0x001000, 0x003000, 0x007000, 0x00f000, 0x01f000, 0x03f000, 0x07f000, 0x0ff000, 0x1ff000, 0x3ff000, 0x7ff000, 0xfff000
    };

    // range: [1111 1111 1111, 1000 0000 0000]
    private static final int [] leftHitMarkers = {
            0xfff, 0xffe, 0xffc, 0xff8, 0xff0, 0xfe0, 0xfc0, 0xf80, 0xf00, 0xe00, 0xc00, 0x800
    };

    // range: [1111 1111 1111, 1000 0000 0000]
    private static final int[] rightHitMarkers = {
            0x001, 0x003, 0x007, 0x00f, 0x01f, 0x03f, 0x07f, 0x0ff, 0x1ff, 0x3ff, 0x7ff, 0xfff
    };

    private SFTable8_12_12(int bucketNum){
        // an entry occupies 32 bits, then a bucket occupies 128 bit
        // thus, we need $[bucketNum * 2] long numbers
        bitSet = new long[bucketNum << 1];
    }

    private SFTable8_12_12(long[] bitSet) {
        this.bitSet = bitSet;
    }

    static SFTable8_12_12 create(int bucketNum) {
        if((bucketNum & (bucketNum - 1)) != 0){
            throw new IllegalArgumentException("bucketNum must be a power of 2");
        }
        return new SFTable8_12_12(bucketNum);
    }

    static SFTable8_12_12 create(long[] bitSet) {
        return new SFTable8_12_12(bitSet);
    }

    @Override
    int getBucketNum(){
        return bitSet.length >> 1;
    }

    @Override
    int getBucketByteSize(){
        return 16;
    }

    @Override
    long getLongValue(int arrayPos) {
        return bitSet[arrayPos];
    }

    @Override
    ByteBuffer serialize(){
        // we only need to store table and bucketNum
        // if we support more table, we need to store table version
        int bucketNum = getBucketNum();
        int bucketSumSize = bucketNum * getBucketByteSize();
        ByteBuffer buffer = ByteBuffer.allocate(bucketSumSize + 4);

        // first write long array length, here len = bucketNum * 2
        int arrayLen = bucketNum << 1;
        buffer.putInt(arrayLen);
        for(int i = 0; i < arrayLen; ++i){
            long longContent = bitSet[i];
            buffer.putLong(longContent);
        }
        buffer.flip();
        return buffer;
    }

    @Override
    boolean insertToBucket(int bucketIndex, long tag){
        // please noe that we allow same fingerprint in a bucket

        // 1-st ~ 12-th bits are hit_marker, which should be zero
        if((tag & 0xffffffff_00_000_fffL) != 0){
            throw new RuntimeException("tag (" + Long.toHexString(tag) + ") is illegal.");
        }

        long fingerprint = tag >> 24;
        for(int i = 0; i < 2; ++i){
            int writePos = (bucketIndex << 1) | i;
            long longNum = bitSet[writePos];
            // if first interval_marker = 0, then insert it directly
            // or if first fingerprint is same, then insert it directly
            boolean insertFirstPos = ((longNum & 0xfff_000L) == 0) | (((longNum >> 24) & 0xff) == fingerprint);
            if(insertFirstPos){
                bitSet[writePos] |= tag;
                return true;
            }else {
                boolean insertSecondPos = ((longNum & 0xfff_000_00000000L) == 0) | (longNum >>> 56) == fingerprint;
                if(insertSecondPos){
                    bitSet[writePos] |= (tag << 32);
                    return true;
                }
            }
        }
        // if this bucket without the same fingerprint or vacant slot, insert tag fails
        return false;
    }

    @Override
    boolean findTag(int i1, int i2, long tag) {
        int arrayPos1 = i1 << 1;
        int arrayPos2 = i2 << 1;
        return hasTag(bitSet[arrayPos1], tag) || hasTag(bitSet[arrayPos1 | 0x1], tag) ||
                hasTag(bitSet[arrayPos2], tag) || hasTag(bitSet[arrayPos2 | 0x1], tag);
    }

    public static boolean hasTag(long x, long tag){
        long mask = (tag & 0xfff000) | 0xff_000_000L;
        return (x & mask) == tag || ((x >>> 32) & mask)== tag;
    }

    @Override
    int altIndex(int bucketIndex, long fingerprint) {
        // 0xc4ceb9fe1a85ec53L hash mixing constant from MurmurHash3
        // Similar value used in: https://github.com/efficient/cuckoofilter/
        // due to fingerprint can be zero, so we add one
        long altIndex = bucketIndex ^ ((fingerprint + 1) * 0xc4ceb9fe1a85ec53L);
        // now pull into valid range
        return hashIndex(altIndex);
    }

    @Override
    long generate(long item){
        // old version:
        // long hashCode = XXHash.hash64(BasicTypeToByteUtils.longToBytes(item), 8, 0);
        long hashCode = QuickHash.hash64(item);
        int fpLen = 8;
        int fingerprint = (int) (hashCode >>> (64 - fpLen));
        int rightShift = 32;
        int bucketIndex = hashIndex(hashCode >> rightShift);
        // low 32 bits are bucketIndex, high 32 bits are fingerprint
        return ((long)fingerprint << 32) | bucketIndex;
    }

    @Override
    long swapRandomTagInBucket(int bucketIndex, long tag){
        // generate random position from {0, 1, 2, 3}
        int randomSlotPosition = ThreadLocalRandom.current().nextInt(4);
        long returnTag;
        int writePos;
        switch(randomSlotPosition){
            case 0:
                writePos = bucketIndex << 1;
                returnTag = bitSet[writePos] & 0xffffffffL;
                bitSet[writePos] = (bitSet[writePos] & 0xffffffff_00000000L) | tag;
                break;
            case 1:
                writePos = bucketIndex << 1;
                returnTag = bitSet[writePos] >>> 32;
                bitSet[writePos] = (bitSet[writePos] & 0x00000000_ffffffffL) | (tag << 32);
                break;
            case 2:
                writePos = (bucketIndex << 1) | 1;
                returnTag = bitSet[writePos] & 0xffffffffL;
                bitSet[writePos] = (bitSet[writePos] & 0xffffffff_00000000L) | tag;
                break;
            default:
                // case 3
                writePos = (bucketIndex << 1) | 1;
                returnTag = bitSet[writePos] >>> 32;
                bitSet[writePos] = (bitSet[writePos] & 0x00000000_ffffffffL) | (tag << 32);
        }
        //System.out.println("returnTag ==> 0x" + Long.toHexString(returnTag));
        return returnTag;
    }

    @Override
    long getTag(long window, long fingerprint, long distance) {
        long tag = fingerprint << 24;
        int ptr = (int) (distance * 12.0 / window);
        return tag | tags[ptr];
    }

    @Override
    int getLeftIntervalMarker(long startTs, long offset, long window) {
        long distance = startTs - offset;
        int ptr = (int) (distance * 12.0 / window);
        return leftIntervalMarkers[ptr];
    }

    @Override
    int getRightIntervalMarker(long endTs, long offset, long window) {
        long distance = endTs - offset;
        int ptr = (int) (distance * 12.0 / window);
        return rightIntervalMarkers[ptr];
    }

    @Override
    int getLeftHitMarker(long startTs, long offset, long window) {
        long distance = startTs - offset;
        int ptr = (int) (distance * 12.0 / window);
        return leftHitMarkers[ptr];
    }

    @Override
    int getRightHitMarker(long endTs, long offset, long window) {
        long distance = endTs - offset;
        int ptr = (int) (distance * 12.0 / window);
        return rightHitMarkers[ptr];
    }

    @Override
    void updateTagInBucket(int i1, int i2, long fingerprint, int hitMarker) {
        //System.out.println("hitMarker: " + Long.toHexString(hitMarker));

        int fpLen = 8;
        int markerLen = 12;

        // due to here may have same fingerprint, so we cannot early break
        // we need to check all fingerprints
        for(int i = 0; i < 4; i++){
            int writePos = i < 2 ? (i1 << 1) | (i & 0x1) : (i2 << 1) | (i & 0x1);
            long longNum = bitSet[writePos];
            long lowFingerprint = (longNum >> (markerLen * 2)) & ((1 << fpLen) - 1);
            long lowIntervalMarker = (longNum >> markerLen) & ((1 << markerLen) - 1);
            long highFingerprint = longNum >>> (64 - fpLen);
            long highIntervalMarker = (longNum >> (32 + markerLen)) & ((1 << markerLen) - 1);

            // check then update hit marker
            if(lowFingerprint == fingerprint && lowIntervalMarker != 0){
                bitSet[writePos] |= hitMarker;
            }
            if(highFingerprint == fingerprint && highIntervalMarker != 0){
                bitSet[writePos] |= ((long) hitMarker << 32);
            }
        }
    }

    int hashIndex(long originIndex) {
        // we always need to return a bucket index within table range
        // we can return low bit because numBuckets is a pow of two
        return (int) (originIndex & ((bitSet.length >> 1) - 1));
    }

    /**
     * merge table, if hit_marker == 0, then we can discard this tag
     * @param xTable - another table
     */
    @Override
    void merge(AbstractSFTable xTable){
        int bucketNum = getBucketNum();
        if(bucketNum != xTable.getBucketNum()){
            throw new RuntimeException("two tables cannot merge, because their bucket number is different, " +
                    "size of this table is " + bucketNum + ", however, size of another table is " + xTable.getBucketNum());
        }
        for(int i = 0; i < bitSet.length; ++i){
            bitSet[i] = bitSet[i] | xTable.getLongValue(i);
        }
        // return this;
    }

    @Override
    int getKeyNum() {
        int count = 0;
        for (long semiBucketContent : bitSet) {
            if((semiBucketContent & 0x00fff000L) != 0){
                count++;
            }
            if((semiBucketContent & 0x00fff000_00000000L) != 0){
                count++;
            }
        }
        return count;
    }

    @Override
    long getSliceNum() {
        long count = 0;
        long mask = 0x00fff000_00fff000L;
        // for each bucket, we count the number of 1 in interval markers
        for (long semiBucketContent : bitSet) {
            count += Long.bitCount(semiBucketContent & mask);
        }
        return count;
    }

    // if a time interval do not be hit, then we can remove this time interval
    // this function will return the number of tags
    // 2024-20-28
    @Override
    int rebuildTable() {
        int tagNum = 0;
        int arrayLen = bitSet.length;
        int bucketNum = getBucketNum();
        int rightShift = arrayLen == bucketNum ? 0 : 1;

        // if we find "and marker" is 0, we need to clear fingerprint
        for(int i = 0; i < arrayLen; i++){
            long lowSlot = bitSet[i] & 0xffffffffL;
            long lowAndMarker = (lowSlot & 0xfff) & (lowSlot >>> 12);
            if(lowAndMarker == 0){
                lowSlot = 0;
            }else{
                lowSlot = (lowSlot & 0xff_000000L) | (lowAndMarker << 12);
                tagNum++;
            }

            long highSlot = bitSet[i] >>> 32;
            long highAndMarker = (highSlot & 0xfff) & (highSlot >>> 12);
            if(highAndMarker == 0){
                highSlot = 0;
            }else{
                highSlot = (highSlot & 0xff_000000L) | (highAndMarker << 12);
                tagNum++;
            }

            bitSet[i] = (highSlot << 32) | lowSlot;
        }

        double maxKeyNum = bucketNum * 4 * 0.78;
        int compactCnt = 0;
        while(tagNum < maxKeyNum * 0.5 && maxKeyNum >= 50){
            compactCnt++;
            maxKeyNum /= 2;
        }
        // System.out.println("shrink filter have reduced the space by " + compactCnt + " times");

        if(compactCnt == 0){
            return 0;
        }

        // to avoid compact fail, we need to save a snapshot
        long[] copiedBitSet = new long[arrayLen];
        System.arraycopy(bitSet, 0, copiedBitSet, 0, bitSet.length);

        arrayLen = arrayLen >> compactCnt;
        long[] semiTable = getSemiTable(compactCnt);
        int tableLen = semiTable.length;
        for(int i = 0; i < tableLen; i++){
            long value = semiTable[i];
            int bucketIdx = (i % arrayLen) >> rightShift;
            for(int j = 0; j < 2; j++){
                long slot = value & 0xffffffffL;
                long fp = slot >> 24;    // please modify this
                int altBucketIdx = altIndex(bucketIdx, fp);
                if(insertToBucket(bucketIdx, slot) || insertToBucket(altBucketIdx, slot)){
                    // we need to rollback
                    bitSet = copiedBitSet;
                    return 0;
                }
                value >>>= 32;
            }
        }

        return compactCnt;
    }

    @Override
    long[] getSemiTable(int compactCnt){
        int splitPos = bitSet.length >> compactCnt;
        long[] part1 = new long[splitPos];
        int returnLen = bitSet.length - splitPos;
        long[] part2 = new long[returnLen];
        System.arraycopy(bitSet, 0, part1, 0, splitPos);
        System.arraycopy(bitSet, splitPos, part2, 0, returnLen);
        bitSet = part1;
        return part2;
    }

    // this function is used for debugging
    @Override
    void displayWithDecimal(){
        int bucketNum = (bitSet.length >> 1);
        for(int bucketIndex = 0; bucketIndex < bucketNum; ++bucketIndex){
            System.out.print(bucketIndex + "-th bucket:");
            for(int i = 0; i < 2; ++i){
                long longNum = bitSet[(bucketIndex << 1) | i];
                long lowFingerprint = (longNum >> 24) & 0x0ff;
                long lowIntervalMarker = (longNum >> 12) & 0x0fff;
                long lowHitMarker = longNum & 0x0fff;
                System.out.print(" (" + lowFingerprint + "," + lowIntervalMarker + "," + lowHitMarker + ")");
                // ---
                long highFingerprint = (longNum >> 56) & 0x0ff;
                long highIntervalMarker = (longNum >> 44) & 0x0fff;
                long highHitMarker = (longNum >> 32) & 0x0fff;
                System.out.println(" (" + highFingerprint + "," + highIntervalMarker + "," + highHitMarker + ")");
            }
            System.out.println();
        }
    }

    // this function is used for debugging
    @Override
    void displayWithHex(){
        int bucketNum = (bitSet.length >> 1);
        for(int bucketIndex = 0; bucketIndex < bucketNum; ++bucketIndex){
            System.out.print(bucketIndex + "-th bucket:");
            long longNum1 = bitSet[bucketIndex << 1];
            long longNum2 = bitSet[(bucketIndex << 1) | 1];
            System.out.print(" 0x" + Long.toHexString(longNum1 & 0xffff_ffffL));
            System.out.print(" 0x" + Long.toHexString(longNum1 >>> 32));
            System.out.print(" 0x" + Long.toHexString(longNum2 & 0xffff_ffffL));
            System.out.print(" 0x" + Long.toHexString(longNum2 >>> 32));
            System.out.println();
        }
    }

    @Override
    AbstractSFTable copy(){
        long[] copyBitSet = new long[bitSet.length];
        System.arraycopy(bitSet, 0, copyBitSet, 0, bitSet.length);
        return new SFTable8_12_12(copyBitSet);
    }
}
