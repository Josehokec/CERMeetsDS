package filter;

import java.nio.ByteBuffer;

/**
 * this is the new version of shrink filter table
 * a table has 4 slots, a slot has three fields: fingerprint, interval_marker, hit_marker
 * this version has a high network overhead and high false positive rate
 * thus, we remove the hit_marker field (see ShrinkFilterUltra)
 */
public abstract class AbstractSFTable {
    abstract int getBucketNum();
    abstract int getBucketByteSize();
    abstract long getLongValue(int arrayPos);
    abstract ByteBuffer serialize();

    /**
     * note that:
     *   1. if two item have same key, then we will merge them
     *   2. if two item have different key, but their hash value is same, we also merge them
     * insert tag, note that fingerprint can be zero, interval_marker cannot be zero
     * @param bucketIndex - bucketIndex
     * @param tag - (fingerprint, interval_marker, hit_marker)
     */
    abstract boolean insertToBucket(int bucketIndex, long tag);

    /**
     * here interval_marker only one bit with a value of 1, hit_marker = 0
     * @param i1 - bucket index 1
     * @param i2 - bucket index 2
     * @param tag - (fingerprint, interval_marker in {1,2,4,8}, hit_marker = 0)
     * @return - fingerprint exists and interval_marker & corresponding position not zero
     */
    abstract boolean findTag(int i1, int i2, long tag);

    abstract int altIndex(int bucketIndex, long fingerprint);

    abstract long generate(long item);

    abstract long swapRandomTagInBucket(int bucketIndex, long tag);

    abstract long getTag(long window, long fingerprint, long distance);

    abstract int getLeftIntervalMarker(long startTs, long offset, long window);

    abstract int getRightIntervalMarker(long endTs, long offset, long window);

    abstract int getLeftHitMarker(long startTs, long offset, long window);

    abstract int getRightHitMarker(long endTs, long offset, long window);

    abstract void updateTagInBucket(int i1, int i2, long fingerprint, int hitMarker);

    abstract void merge(AbstractSFTable xTable);

    abstract int getKeyNum();

    abstract long getSliceNum();
    /**
     * insert -> update -> rebuild
     * this functions aims to delete the intervals that don't contain matched results
     */
    abstract int rebuildTable();

    abstract long[] getSemiTable(int compactCnt);

    // this function is used for debugging
    abstract void displayWithDecimal();

    // this function is used for debugging
    abstract void displayWithHex();

    abstract AbstractSFTable copy();
}
