package filter;

import hasher.XXHash;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * window-wise bloom filter: windowID as hash seed, and attribute value as key
 */
public class WindowWiseBF implements BasicBF {
    private final long[] longArray; // we use long array to simulate bit array
    private final int k;            // number of hash functions
    private final int bitSize;      // bit array size

    public WindowWiseBF(double fpr, int itemNum) {
        // k = ceil(-log_2(fpr))
        double value = -(Math.log(fpr) / Math.log(2));
        k = (int) Math.ceil(value);
        int estimatedBitSize = (int) Math.ceil(1.44 * itemNum * value);
        longArray = new long[(int) ((estimatedBitSize + 63) / 64)];
        bitSize = longArray.length * 64;
    }

    public WindowWiseBF(ByteBuffer buffer){
        this.k = buffer.getInt();
        int longValueNum = buffer.getInt();
        long[] longValues = new long[longValueNum];
        LongBuffer longBuffer = buffer.asLongBuffer();
        longBuffer.get(longValues);
        this.longArray = longValues;
        this.bitSize = longValues.length * 64;
    }

    public static int getEstimatedBitSize(double itemNum){
        double fpr = 0.01;     // double DEFAULT_FPR = 0.01; EventCache.java 177 line
        double value = -(Math.log(fpr) / Math.log(2));
        return (int) Math.ceil(1.44 * itemNum * value);
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
        byte[] data = key.getBytes();
        long hashValue = XXHash.hash64(data, data.length, windowId);
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
        byte[] data = key.getBytes();
        long hashValue = XXHash.hash64(data, data.length, windowId);
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

        // update position
        // buffer.position(buffer.position() + longValueNum * 8);
        // longBuffer.flip();
        buffer.rewind();
        // System.out.println("longArray: " + Arrays.toString(longArray));
        return buffer;
    }

    public static WindowWiseBF deserialize(ByteBuffer buffer) {
        return new WindowWiseBF(buffer);
    }
}



//        int k = buffer.getInt();
//        int longValueNum  = buffer.getInt();
//        long[] longValues = new long[longValueNum];
//        for(int i = 0; i < longValues.length; ++i){
//            longValues[i] = buffer.getLong();
//        }