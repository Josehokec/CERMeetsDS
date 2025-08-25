package filter;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * assume that f = len(fingerprint), m = len(interval_marker)
 * and a bucket has 128 bits, then fpr = \alpha * 8 / (2 ** f) +  (1-\alpha) * 1 / m
 * we can know that f = 12, m = 20, \alpha=0.5 fpr has minimum value: 0.026
 */
public class SFTable12_20 {
    private long[] bitSet;

    private SFTable12_20(int bucketNum){
        // a bucket occupies 128 bits
        bitSet = new long[bucketNum << 1];
    }

    private SFTable12_20(long[] bitSet){
        this.bitSet = bitSet;
    }

    public static SFTable12_20 createTable(int bucketNum){
        // only used for debugging
        if((bucketNum & (bucketNum - 1)) != 0){
            throw new IllegalArgumentException("bucketNum must be a power of 2, current bucketNum = " + bucketNum);
        }
        return new SFTable12_20(bucketNum);
    }

    public static SFTable12_20 createTable(long[] bitSet){
        return new SFTable12_20(bitSet);
    }

    public int getBucketNum() {
        return bitSet.length >> 1;
    }

    public int getBucketByteSize() {
        return 16;
    }

    public ByteBuffer serialize() {
        int bucketNum = getBucketNum();
        int bucketSumSize = bucketNum * getBucketByteSize();
        ByteBuffer buffer = ByteBuffer.allocate(bucketSumSize + 4);
        // a bucket has two long value
        buffer.putInt(bucketNum << 1);
        for(long val : bitSet){
            buffer.putLong(val);
        }
        buffer.flip();
        return buffer;
    }

    // you should first call findTag() function to get the bucket index
    // then call insertToBucket function
    public boolean insertToBucket(int bucketIndex, long tag) {
        // tag = <fingerprint, interval_marker>, below 3 code lines only used for debugging
        //if((tag & 0xfffff) == 0 || (tag > 0xffffffffL)){
        //    throw new RuntimeException("tag (" + Long.toBinaryString(tag) + ") is illegal.");
        //}

        // due to we have call findFingerprint function, we know without same fingerprint,
        // then we only need to find a vacant slot (i.e., intervalMarker = 0)
        for(int i = 0; i < 2; i++){
            int idx = i == 0 ? bucketIndex << 1 : (bucketIndex << 1) + 1;
            long val = bitSet[idx];
            if((val & 0xfffff) == 0){
                bitSet[idx] |= tag;
                return true;
            }
            if(((val >> 32) & 0xfffff) == 0){
                bitSet[idx] |= (tag << 32);
                return true;
            }
        }
        return false;
    }

    public boolean insertToBucket(int arrayPos, int highOrLow, long tag){
        if(highOrLow == 0){
            bitSet[arrayPos] |= tag;
        }else{
            bitSet[arrayPos] |= (tag << 32);
        }
        return true;
    }

    public boolean findTag(int i1, int i2, long tag) {
        // last 8 bits should be 0, and 9~16bits cannot be 0
        //if (tag > 0xffffffffL || (((tag & 0xfffffL) & ((tag & 0xfffffL) - 1)) != 0)) {
        //    throw new RuntimeException("tag (" + Long.toBinaryString(tag) + ") is illegal.");
        //}
        return hasTag(bitSet[i1 << 1], tag) || hasTag(bitSet[(i1 << 1) | 0x1], tag) ||
                hasTag(bitSet[i2 << 1], tag) || hasTag(bitSet[(i2 << 1) | 0x1], tag);
        //long intervalMarker = tag & 0xfffffL;
        //long mask = (intervalMarker << 32) | intervalMarker | 0xfff_00000_fff_00000L;
        //return hasValue32Bits(bitSet[i1 << 1] & mask, tag) || hasValue32Bits(bitSet[(i1 << 1) | 0x1] & mask, tag) ||
        //        hasValue32Bits(bitSet[i2 << 1] & mask, tag) || hasValue32Bits(bitSet[(i2 << 1) | 0x1] & mask, tag);
    }

    public static long hasZero32Bits(long x){
        // Similar value used in: https://github.com/efficient/cuckoofilter/blob/master/src/bitsutil.h
        return (((x)-0x0000_0001_0000_0001L) & (~(x)) & 0x8000_0000_8000_0000L);
    }

    public static boolean hasValue32Bits(long x, long n){
        // Similar value used in: https://github.com/efficient/cuckoofilter/blob/master/src/bitsutil.h
        return (hasZero32Bits((x) ^ (0x0000_0001_0000_0001L * (n)))) != 0;
    }

//    public static long hasZeroFP12Bits(long x){
//        return (((x)-0x001_00000_001_00000L) & (~(x)) & 0x800_00000_800_00000L);
//    }
//
//    public static boolean hasFP12Bits(long x, long n){
//        return (hasZeroFP12Bits((x) ^ (0x001_00000_001_00000L * (n)))) != 0;
//    }

    public static boolean hasTag(long x, long tag){
        // n is tag
        long mask = (tag & 0xfffff) | 0xfff00000L;
        return (x & mask) == tag || ((x >>> 32) & mask)== tag;
    }

    /**
     * faster find fingerprint position
     * if you want ultra faster, you can modify code to return
     * arrayPos and high/low position rather than bucketIdx and slotIdx
     * then you also need to modify the logical UpdatedMarker
     * @param bucketIdx1    1-st bucket index
     * @param bucketIdx2    2-nd bucket index
     * @param fp            fingerprint
     * @return composed key: arrayPosition (high 30 bits) + highOrLow marker (slow 2 bits) -> 32 bits (you can change 64 bits)
     */
    public int findFingerprint(int bucketIdx1, int bucketIdx2, long fp){
        long mask = 0xffffffffL;
        long threshold = 0x100000;
        int shift20 = 20;
        int shift52 = 52;

        int arrayPos1 = bucketIdx1 << 1;
        int arrayPos2 = bucketIdx2 << 1;
        for (int i = 0; i < 4; i++) {
            // 0, 1, 2, 3
            int arrayPos = i < 2 ? arrayPos1 | (i & 0x1) : arrayPos2 | (i & 0x1);
            long v = bitSet[arrayPos];
            // add if(v != 0){...}

            if (fp == 0) {
                long v1 = v & mask;
                long v2 = v >>> 32;
                if ((v1 < threshold && v1 > 0) || (v2 < threshold && v2 > 0)) {
                    int j = (v2 < threshold && v2 > 0) ? 1 : 0;
                    return (arrayPos << 2) | j;
                }
            } else {
                // hasFP12Bits(v, fp) VS. ((v >> shift20) & 0xfff) == fp || ((v >>> shift52) == fp)
                if(((v >> shift20) & 0xfff) == fp || ((v >>> shift52) == fp)) {
                    int j = (v >>> shift52) == fp ? 1 : 0;
                    return (arrayPos << 2) | j;
                }
            }
        }

        // return 0011 means we cannot find
        return 0x3;
    }

    long swapRandomTagInBucket(int bucketIndex, long tag) {
        // generate random position from {0, 1, 2, 3}, 3 has bug
        int randomSlotPosition = ThreadLocalRandom.current().nextInt(4);
        long returnTag;
        int writePos;
        //System.out.println("swap tag: " + Long.toHexString(tag) + " swap pos: " +  randomSlotPosition);
        switch(randomSlotPosition){
            case 0:
                writePos = bucketIndex << 1;
                returnTag = bitSet[writePos] & 0xffffffffL;
                bitSet[writePos] = (bitSet[writePos] & 0xffffffff00000000L) | tag;
                break;
            case 1:
                writePos = bucketIndex << 1;
                returnTag = (bitSet[writePos] >> 32)  & 0xffffffffL;
                bitSet[writePos] = (bitSet[writePos] & 0x00000000ffffffffL) | (tag << 32);
                break;
            case 2:
                writePos = (bucketIndex << 1) + 1;
                returnTag = bitSet[writePos] & 0xffffffffL;
                bitSet[writePos] = (bitSet[writePos] & 0xffffffff00000000L) | tag;
                break;
            default:
                // case 3
                writePos = (bucketIndex << 1) + 1;
                returnTag = (bitSet[writePos] >> 32) & 0xffffffffL;
                bitSet[writePos] = (bitSet[writePos] & 0x00000000ffffffffL) | (tag << 32);
        }
        //System.out.println("returnTag ==> 0x" + Long.toHexString(returnTag));
        return returnTag;
    }

    public int updateMarkers(UpdatedMarkers updatedMarkers){
        int keyNum = 0;
        int len = bitSet.length;
        for(int pos = 0; pos < len; pos++){
            bitSet[pos] &= updatedMarkers.getMaskedLongValue(pos);
            if((bitSet[pos] & 0xfffff) == 0){
                bitSet[pos] &= 0xffffffff_00000000L;
            }else{
                keyNum++;
            }

            if((bitSet[pos] & 0xfffff_00000000L) == 0){
                bitSet[pos] &= 0x00000000_ffffffffL;
            }else{
                keyNum++;
            }

        }
        return keyNum;
    }

    public int getKeyNum(){
        int count = 0;
        for(long val : bitSet){
            if((val & 0xfffffL) != 0){
                count++;
            }
            if((val & 0x000fffff00000000L) != 0){
                count++;
            }
        }
        return count;
    }

    public long getSliceNum() {
        long count = 0;
        for(long val : bitSet){
            count += Long.bitCount(val & 0x000fffff000fffffL);
        }
        return count;
    }

    // before compacting we need to get
    public long[] getSemiTable(int compactCnt) {
        int splitPos = bitSet.length >> compactCnt;
        long[] part1 = new long[splitPos];

        int returnLen = bitSet.length - splitPos;
        long[] part2 = new long[returnLen];
        System.arraycopy(bitSet, 0, part1, 0, splitPos);
        System.arraycopy(bitSet, splitPos, part2, 0, returnLen);
        bitSet = part1;
        return part2;
    }

    public void displayWithHex() {
        int bucketNum = (bitSet.length >> 1);
        for(int bucketIndex = 0; bucketIndex < bucketNum; bucketIndex++){
            System.out.print(bucketIndex + "-th bucket:");
            int pos = bucketIndex << 1;
            long val1 = bitSet[pos];
            long val2 = bitSet[pos + 1];
            System.out.print(" 0x" + Long.toHexString(val1 & 0xffffffffL));
            System.out.print(" 0x" + Long.toHexString((val1 >> 32) & 0xffffffffL));
            System.out.print(" 0x" + Long.toHexString(val2 & 0xffffffffL));
            System.out.print(" 0x" + Long.toHexString((val2 >> 32) & 0xffffffffL));
            System.out.println();
        }
    }

    public SFTable12_20 copy() {
        long[] copyBitSet = new long[bitSet.length];
        System.arraycopy(bitSet, 0, copyBitSet, 0, bitSet.length);
        return new SFTable12_20(copyBitSet);
    }
}