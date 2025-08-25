package filter;

public class CFUtils {
    /**
     * Calculates how many buckets are needed to hold the chosen number of keys,
     * taking the standard load factor into account.
     * @param maxKeys - number of keys the filter is expected to hold before insertion failure.
     * @return - number of buckets needed
     */
    public static int getBucketsNeeded(long maxKeys,double loadFactor) {
        /*
         * force a power-of-two bucket count so hash functions for bucket index
         * can hashBits%numBuckets and get randomly distributed index. See wiki
         * "Modulo Bias". Only time we can get perfectly distributed index is
         * when numBuckets is a power of 2.
         */
        long bucketsNeeded = (int) Math.ceil((1.0 / loadFactor) * maxKeys / 4);
        // get next biggest power of 2
        long curBucketNum = Long.highestOneBit(bucketsNeeded);
        if (bucketsNeeded > curBucketNum)
            curBucketNum = curBucketNum << 1;
        if(curBucketNum > Integer.MAX_VALUE){
            throw new RuntimeException("too large number of buckets, it will incur out of memory exception");
        }
        return (int) curBucketNum;
    }
}
