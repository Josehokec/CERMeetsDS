package filter;

import hasher.QuickHash;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Shrink Filter Table (SFT)
 * the tag (entry) of SFT is:
 * ---- fingerprint (12 bits) + interval_marker (10 bits) + hit_marker (10 bits)
 * bucket size of SFT is 4,which means a bucket of SFT occupies 128 bits
 * bucket storage format:    |entry1|entry0|
 *                           |entry3|entry2|
 */
public class SFTable12_10_10 extends AbstractSFTable {
    private long[] bitSet;

    // ptr: 0, 1, 2, 3, ..., 9
    // example: interval_marker, hit_marker: 0000 0000 01_00 0000 0000 -> 0x400
    private static final int[] tags = {
            0x00400, 0x00800, 0x01000, 0x02000, 0x04000, 0x8000, 0x10000, 0x20000, 0x40000, 0x80000
    };

    // example: 11 1111 1111 -> 0x3ff, 10 0000 0000 -> 0x200
    private static final int[] leftIntervalMarkers = {
            0xffc00, 0xff800, 0xff000, 0xfe000, 0xfc000, 0xf8000, 0xf0000, 0xe0000, 0xc0000, 0x80000
    };

    // range: [0000 0000 0100 0000 0000, 1111 1111 1100 0000 0000]
    private static final int[] rightIntervalMarkers = {
            0x00400, 0x00c00, 0x01c00, 0x03c00, 0x07c00, 0x0fc00, 0x1fc00, 0x3fc00, 0x7fc00, 0xffc00
    };

    // range: [0011 1111 1111 , 0010 0000 0000]
    private static final int [] leftHitMarkers = {
            0x3ff, 0x3fe, 0x3fc, 0x3f8, 0x3f0, 0x3e0, 0x3c0, 0x380, 0x300, 0x200
    };

    // range: [0000 0000 0001, 0011, 1111, 1111]
    private static final int[] rightHitMarkers = {
            0x001, 0x003, 0x007, 0x00f, 0x01f, 0x03f, 0x07f, 0x0ff, 0x1ff, 0x3ff
    };


    private SFTable12_10_10(int bucketNum){
        // an entry occupies 32 bits, then a bucket occupies 128 bit
        // thus, we need $[bucketNum * 2] long numbers
        bitSet = new long[bucketNum << 1];
    }

    private SFTable12_10_10(long[] bitSet){
        this.bitSet = bitSet;
    }

    static SFTable12_10_10 create(int bucketNum){
        if((bucketNum & (bucketNum - 1)) != 0){
            throw new IllegalArgumentException("bucketNum must be a power of 2");
        }
        return new SFTable12_10_10(bucketNum);
    }

    static SFTable12_10_10 create(long[] bitSet){
        return new SFTable12_10_10(bitSet);
    }

    @Override
    int getBucketNum() {
        return bitSet.length >> 1;
    }

    @Override
    int getBucketByteSize() {
        return 16;
    }

    @Override
    long getLongValue(int arrayPos) {
        return bitSet[arrayPos];
    }

    @Override
    ByteBuffer serialize() {
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
    boolean insertToBucket(int bucketIndex, long tag) {
        // please noe that we allow same fingerprint in a bucket

        // debug, lowest 10 bits are hit_marker, which should be zero
        if((tag & 0xffffffff_00_0003ffL) != 0){
            throw new RuntimeException("tag (" + Long.toHexString(tag) + ") is illegal.");
        }

        long fingerprint = tag >> 20;
        for(int i = 0; i < 2; ++i){
            int writePos = (bucketIndex << 1) | i;
            long longNum = bitSet[writePos];
            // if first interval_marker = 0, then insert it directly
            // or if first fingerprint is same, then insert it directly
            boolean insertFirstPos = ((longNum & 0xffc00L) == 0) | (((longNum >> 20) & 0xfff) == fingerprint);
            if(insertFirstPos){
                bitSet[writePos] |= tag;
                return true;
            }else {
                boolean insertSecondPos = ((longNum & 0x000ffc00_00000000L) == 0) | ((longNum >>> 52) == fingerprint);
                if(insertSecondPos){
                    bitSet[writePos] |= (tag << 32);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    boolean findTag(int i1, int i2, long tag) {
        int arrayPos1 = i1 << 1;
        int arrayPos2 = i2 << 1;
        return hasTag(bitSet[arrayPos1], tag) || hasTag(bitSet[arrayPos1 | 0x1], tag) ||
                hasTag(bitSet[arrayPos2], tag) || hasTag(bitSet[arrayPos2 | 0x1], tag);

        // old version code
        //long marker = tag & 0xffc00L; // 1111 1111 1100 0000 0000
        //long mask = (marker << 32) | marker | 0xfff_00000_fff_00000L;
        //return hasValue32Bits(bitSet[i1 << 1] & mask, tag) || hasValue32Bits(bitSet[(i1 << 1) | 0x1] & mask, tag) ||
        //        hasValue32Bits(bitSet[i2 << 1] & mask, tag) || hasValue32Bits(bitSet[(i2 << 1) | 0x1] & mask, tag);
    }

    private static long hasZero32Bits(long x){
        // Similar value used in: https://github.com/efficient/cuckoofilter/blob/master/src/bitsutil.h
        return (((x)-0x0000_0001_0000_0001L) & (~(x)) & 0x8000_0000_8000_0000L);
    }

    private static boolean hasValue32Bits(long x, long n){
        // Similar value used in: https://github.com/efficient/cuckoofilter/blob/master/src/bitsutil.h
        return (hasZero32Bits((x) ^ (0x0000_0001_0000_0001L * (n)))) != 0;
    }

    public static boolean hasTag(long x, long tag){
        long mask = (tag & 0xffc00) | 0xfff00000L;
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
    long generate(long item) {
        // old version:
        // long hashCode = XXHash.hash64(BasicTypeToByteUtils.longToBytes(item), 8, 0);
        long hashCode = QuickHash.hash64(item);
        int fpLen = 12;
        int fingerprint = (int) (hashCode >>> (64 - fpLen));
        int rightShift = 32;
        int bucketIndex = hashIndex(hashCode >> rightShift);
        // low 32 bits are bucketIndex, high 32 bits are fingerprint
        return ((long)fingerprint << 32) | bucketIndex;
    }

    @Override
    long swapRandomTagInBucket(int bucketIndex, long tag) {
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
        return returnTag;
    }

    @Override
    long getTag(long window, long fingerprint, long distance) {
        long tag = fingerprint << 20;
        int ptr = (int) (distance * 10.0 / window);
        return tag | tags[ptr];
    }

    @Override
    int getLeftIntervalMarker(long startTs, long offset, long window) {
        long distance = startTs - offset;
        int ptr = (int) (distance * 10.0 / window);
        return leftIntervalMarkers[ptr];
    }

    @Override
    int getRightIntervalMarker(long endTs, long offset, long window) {
        long distance = endTs - offset;
        int ptr = (int) (distance * 10.0 / window);
        return rightIntervalMarkers[ptr];
    }

    @Override
    int getLeftHitMarker(long startTs, long offset, long window) {
        long distance = startTs - offset;
        int ptr = (int) (distance * 10.0 / window);
        return leftHitMarkers[ptr];
    }

    @Override
    int getRightHitMarker(long endTs, long offset, long window) {
        long distance = endTs - offset;
        int ptr = (int) (distance * 10.0 / window);
        return rightHitMarkers[ptr];
    }

    @Override
    void updateTagInBucket(int i1, int i2, long fingerprint, int hitMarker) {
        //System.out.println("hitMarker: " + Long.toHexString(hitMarker));

        // due to here may have same fingerprint, so we cannot early break
        // we need to check all fingerprints
        for(int i = 0; i < 4; i++){
            // bugs
            int writePos = i < 2 ? ((i1 << 1) | (i & 0x1)): ((i2 << 1) | (i & 0x1));
            long longNum = bitSet[writePos];
            long lowFingerprint = (longNum >> 20) & 0xfff;
            long lowIntervalMarker = (longNum >> 10) & 0x3ff;
            long highFingerprint = longNum >>> 52;
            long highIntervalMarker = (longNum >> 42) & 0x3ff;

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

    @Override
    void merge(AbstractSFTable xTable) {
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
            if((semiBucketContent & 0x000ffc00L) != 0){
                count++;
            }
            if((semiBucketContent & 0x000ffc00_00000000L) != 0){
                count++;
            }
        }
        return count;
    }

    @Override
    long getSliceNum() {
        long count = 0;
        long mask = 0x000ffc00_000ffc00L;
        // for each bucket, we count the number of 1 in interval markers
        for (long semiBucketContent : bitSet) {
            count += Long.bitCount(semiBucketContent & mask);
        }
        return count;
    }

    @Override
    int rebuildTable() {

        int tagNum = 0;
        int arrayLen = bitSet.length;
        int bucketNum = getBucketNum();
        int rightShift = arrayLen == bucketNum ? 0 : 1;

        // if we find "and marker" is 0, we need to clear fingerprint
        for(int i = 0; i < arrayLen; i++){
            long lowSlot = bitSet[i] & 0xffffffffL;
            long lowAndMarker = (lowSlot & 0x3ff) & (lowSlot >>> 10);
            if(lowAndMarker == 0){
                lowSlot = 0;
            }else{
                lowSlot = (lowSlot & 0xfff_00000L) | (lowAndMarker << 10);
                tagNum++;
            }

            long highSlot = bitSet[i] >>> 32;
            long highAndMarker = (highSlot & 0x3ff) & (highSlot >>> 10);
            if(highAndMarker == 0){
                highSlot = 0;
            }else{
                highSlot = (highSlot & 0xfff_00000L) | (highAndMarker << 10);
                tagNum++;    // bug find 2025-3-8
            }

            bitSet[i] = (highSlot << 32) | lowSlot;
        }

        //debug
        // System.out.println("tagNum: " + tagNum + " entry num: " + bucketNum * 4);

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
                long fp = slot >> 20;
                int altBucketIdx = altIndex(bucketIdx, fp);
                if(insertToBucket(bucketIdx, slot) || insertToBucket(altBucketIdx, slot)){
                    // we need to rollback
                    System.out.println("rollback");
                    bitSet = copiedBitSet;
                    return 0;
                }
                value >>>= 32;
            }
        }

        return compactCnt;
    }

    // when load factor lower than LOAD_FACTOR * 0.5, we can choose to compress table
    // return <fp,tag> from second semi-bucket, we need tag length <= 32
    @Override
    long[] getSemiTable(int compactCnt) {
        int splitPos = bitSet.length >> compactCnt;
        long[] part1 = new long[splitPos];
        int returnLen = bitSet.length - splitPos;
        long[] part2 = new long[returnLen];
        System.arraycopy(bitSet, 0, part1, 0, splitPos);
        System.arraycopy(bitSet, splitPos, part2, 0, returnLen);
        bitSet = part1;
        return part2;
    }

    @Override
    void displayWithDecimal() {
        int bucketNum = (bitSet.length >> 1);
        for(int bucketIndex = 0; bucketIndex < bucketNum; ++bucketIndex){
            System.out.print(bucketIndex + "-th bucket:");
            for(int i = 0; i < 2; ++i){
                long longNum = bitSet[(bucketIndex << 1) | i];
                long lowFingerprint = (longNum >> 20) & 0x0ff;
                long lowIntervalMarker = (longNum >> 10) & 0x03ff;
                long lowHitMarker = longNum & 0x3ff;
                System.out.print(" (" + lowFingerprint + "," + lowIntervalMarker + "," + lowHitMarker + ")");
                // ---
                long highFingerprint = (longNum >> 56) & 0x0ff;
                long highIntervalMarker = (longNum >> 44) & 0x03ff;
                long highHitMarker = (longNum >> 32) & 0x3ff;
                System.out.println(" (" + highFingerprint + "," + highIntervalMarker + "," + highHitMarker + ")");
            }
            System.out.println();
        }
    }

    @Override
    void displayWithHex() {
        int bucketNum = (bitSet.length >> 1);
        //debug
//        for(int bucketIndex = 0; bucketIndex < bucketNum; ++bucketIndex){
        for(int bucketIndex = 0; bucketIndex < 10; ++bucketIndex){
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
    AbstractSFTable copy() {
        long[] copyBitSet = new long[bitSet.length];
        System.arraycopy(bitSet, 0, copyBitSet, 0, bitSet.length);
        return new SFTable12_10_10(copyBitSet);
    }
}
