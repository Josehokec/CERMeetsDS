package filter;

import hasher.XXHash;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class StandardBF implements BasicBF{
    // we use long array to simulate bit array
    private final long[] longArray;
    private final int k;
    private final int bitSize; // number of hash functions
    private static final int fixedSeed = 7;

    public StandardBF(double fpr, int itemNum) {
        // k = ceil(-log_2(fpr))
        double value = -(Math.log(fpr) / Math.log(2));
        k = (int) Math.ceil(value);
        bitSize = (int) Math.ceil(1.44 * itemNum * value);
        longArray = new long[(int) ((bitSize + 63) / 64)];
    }

    public StandardBF(ByteBuffer buffer) {
        this.k = buffer.getInt();
        int longValueNum = buffer.getInt();
        long[] longValues = new long[longValueNum];
        LongBuffer longBuffer = buffer.asLongBuffer();
        longBuffer.get(longValues);
        // System.out.println("longArray: " + Arrays.toString(longValues));
        this.longArray = longValues;
        this.bitSize = longValues.length * 64;
    }

    private void setBit(int index) {
        int arrayIndex = index >>> 6;               // long array position, pow(2, 6)=64
        int bitPosition = index & 0x3f;             // get last 6 bits
        longArray[arrayIndex] |= (1L << bitPosition);
    }

    private boolean getBit(int index) {
        int arrayIndex =  index >>> 6;
        int bitPosition = index & 0x3f;
        return (longArray[arrayIndex] & (1L << bitPosition)) != 0;
    }

    @Override
    public void insert(String key, long windowId) {
        String combineKey = key + "-" + windowId;
        byte[] data = combineKey.getBytes();
        long hashValue = XXHash.hash64(data, data.length, fixedSeed);
        long hasPosition = hashValue & 0xffffffffL;
        long high32bit = hashValue >>> 32;

        for(int i = 0; i < k; ++i){
            // to avoid generate negative number, we need to use AND operator
            setBit((int) (hasPosition % bitSize));
            hasPosition += high32bit;
        }
    }

    @Override
    public boolean contain(String key, long windowId) {
        String combineKey = key + "-" + windowId;
        byte[] data = combineKey.getBytes();
        long hashValue = XXHash.hash64(data, data.length, fixedSeed);
        long hasPosition = hashValue & 0xffffffffL;
        long high32bit = hashValue >>> 32;
        for(int i = 0; i < k; ++i){
            if(!getBit((int) (hasPosition % bitSize))){
                return false;
            }
            hasPosition += high32bit;
        }
        return true;
    }

    @Override
    public ByteBuffer serialize() {
        // we store k, length of long array, and long array
        int longValueNum = longArray.length;
        ByteBuffer buffer = ByteBuffer.allocate(8 + longValueNum * 8);

        // write head information
        buffer.putInt(k);
        buffer.putInt(longValueNum);
        LongBuffer longBuffer = buffer.asLongBuffer();
        longBuffer.put(longArray);
        buffer.rewind();
        return buffer;
    }

    public static StandardBF deserialize(ByteBuffer buffer) {
//        int k = buffer.getInt();
//        int longValueNum  = buffer.getInt();
//        long[] longValues = new long[longValueNum];
//        for(int i = 0; i < longValues.length; ++i){
//            longValues[i] = buffer.getLong();
//        }
        return new StandardBF(buffer);
    }
}
