package filter;

import java.nio.ByteBuffer;

/**
 * default setting: MARKER_LEN = 20;
 * please note that we use a long value to store two slot content
 */
public class UpdatedMarkers {
    // a long value store two markers
    private final long[] markerBucket;
    private int filteredEventNum;

    public UpdatedMarkers(int bucketNum){
        markerBucket = new long[bucketNum << 1];
        filteredEventNum = 0;
    }

    public int getFilteredEventNum(){
        return filteredEventNum;
    }

    public UpdatedMarkers(long[] markerBucket){
        this.markerBucket = markerBucket;
    }

    public long getLongValue(int pos){
        return markerBucket[pos];
    }

    public long getMaskedLongValue(int pos){
        // if modify default setting, you should modify below code lines
        // fingerprint is 12 bits, marker is 20 bits
        return markerBucket[pos] | 0xfff_00000_fff_00000L;
    }

    public int getBucketNum(){
        return markerBucket.length >> 1;
    }

    public void update(int arrayPos, int highOrLow, long marker){
        if(highOrLow == 0){
            markerBucket[arrayPos] |= marker;
        }else{
            markerBucket[arrayPos] |= (marker << 32);
        }
    }

    public void merge(UpdatedMarkers other){
        filteredEventNum += other.getFilteredEventNum();
        int len = markerBucket.length;
        for (int i = 0; i < len; i++){
            markerBucket[i] |= other.getLongValue(i);
        }
    }

    static class CompressedBitSet {
        private long[] bitSet;
        private int arrayIdx;
        private int longBitIdx;
        private int slotNum;

        public CompressedBitSet(int slotNum){
            bitSet = new long[(slotNum / 3) + 1];
            arrayIdx = 0;
            longBitIdx = 0;
            this.slotNum = slotNum;
        }

        public void write(long semiBucketContent){
            long v1 = semiBucketContent & 0xfffff;
            long v2 = semiBucketContent >>> 32;
            // four cases: 00, (20 bits)10, 0(20bits)1, (20 bits)1(20 bits)1
            long value;
            int bitLen;
            if(v1 == 0){
                if(v2 == 0){
                    value = 0;
                    bitLen = 2;
                }else{
                    value = (v2 << 2) | 2;
                    bitLen = 22;
                }
            }else{
                if(v2 == 0){
                    value = (v1 << 1) | 1;
                    bitLen = 22;
                }else{
                    value = (v1 << 1) | (v2 << 22) | 0x200001;
                    bitLen = 42;
                }
            }

            int longRemainBits = 64 - longBitIdx;
            if(longRemainBits >= bitLen){
                bitSet[arrayIdx] |= (value << longBitIdx);
                longBitIdx += bitLen;
                if(longBitIdx == 64){
                    longBitIdx = 0;
                    arrayIdx++;
                }
            }else{
                long newLongVal = (value & ((1L << longRemainBits) - 1)) << longBitIdx;
                bitSet[arrayIdx] |= newLongVal;
                arrayIdx++;
                bitSet[arrayIdx] = value >>> longRemainBits;
                longBitIdx = bitLen - longRemainBits;
            }
        }

        // new version: add filteredEventNum
        ByteBuffer getByteBuffer(){
            int longValNum = (longBitIdx == 0 ? arrayIdx : arrayIdx + 1);
            int size = 4 + (longValNum << 3);
            ByteBuffer bb = ByteBuffer.allocate(size);
            bb.putInt(slotNum);
            for(int i = 0; i < longValNum; i++){
                bb.putLong(bitSet[i]);
            }
            bb.flip();
            return bb;
        }

        public static long[] read(ByteBuffer bb){
            int slotNum = bb.getInt();
            // a long value can store 2 slots
            long[] values = new long[slotNum >> 1];
            int hasReadSlot = 0;
            int readLongBitIdx = 0;
            long v = bb.getLong();

            while(hasReadSlot < slotNum){
                if(readLongBitIdx == 64){
                    v = bb.getLong();
                    readLongBitIdx = 0;
                }

                if((v & (1L << readLongBitIdx)) == 0){
                    // we only read 1 bit
                    hasReadSlot++;
                    readLongBitIdx++;
                }else{
                    int remainBitLen = 63 - readLongBitIdx;
                    if(remainBitLen >= 20){
                        long tag = (v >> (readLongBitIdx + 1)) & 0xfffff;
                        values[hasReadSlot >> 1] |= (hasReadSlot & 1) == 1 ? (tag << 32) : tag;
                        hasReadSlot++;
                        readLongBitIdx += 21;
                    }else{
                        long tag = (v >> (readLongBitIdx + 1)) & ((1L << remainBitLen) - 1);
                        v = bb.getLong();
                        readLongBitIdx = (20 - remainBitLen);
                        tag |= ((v & ((1L << readLongBitIdx) - 1)) << remainBitLen);
                        values[hasReadSlot >> 1] |= (hasReadSlot & 1) == 1 ? (tag << 32) : tag;
                        hasReadSlot++;
                    }
                }
            }
            return values;
        }
    }

    public ByteBuffer serialize(){
        int slotNum = getBucketNum() << 2;
        // a long value can store 3 slot content
        CompressedBitSet bitSet = new CompressedBitSet(slotNum);
        for (long val : markerBucket) {
            //System.out.println("semi bucket: " + Long.toHexString(val));
            bitSet.write(val);
        }
        return bitSet.getByteBuffer();
    }

    public static UpdatedMarkers deserialize(ByteBuffer buffer){
        long[] values = CompressedBitSet.read(buffer);
        return new UpdatedMarkers(values);
    }

    public void displayWithHex(){
        int bucketNum = getBucketNum();
        for(int bucketIdx = 0; bucketIdx < bucketNum; bucketIdx++){
            System.out.print(bucketIdx + "-th bucket: ");
            long val1 = getLongValue(bucketIdx << 1);
            long val2 = getLongValue((bucketIdx << 1) + 1);
            System.out.println(Long.toHexString(val1 & 0xfffff) + ", " + Long.toHexString((val1 >> 32) & 0xfffff) +
                    ", " + Long.toHexString(val2 & 0xfffff) + ", " + Long.toHexString((val2 >> 32) & 0xfffff));
        }
    }
}

/*
We also provide the simple serialize and deserialize methods
//    public ByteBuffer serialize(){
//        // len(marker) = 20, then a long value can store 3 slot content
//        int slotNum = getBucketNum() << 2;
//        int spaceSize = (int) (8 *  Math.ceil(slotNum / 3.0) + 4);
//        ByteBuffer buffer = ByteBuffer.allocate(spaceSize);
//        buffer.putInt(slotNum);
//        int cnt = 0;
//        int shift = 0;
//        long writeVal = 0;
//
//        // low bit -> high bit
//        // |slot0|slot1|slot2|x|
//        // |slot3|slot4|slot5|x|
//
//        for (long val : markerBucket) {
//            long v1 = val & 0xfffff;
//            writeVal += (v1 << shift);
//            cnt++;
//            if(cnt == 3){
//                buffer.putLong(writeVal);
//                writeVal = 0;
//                shift = 0;
//                cnt = 0;
//            }else{
//                shift += 20;
//            }
//
//            long v2 = (val >> 32) & 0xfffff;
//            writeVal += (v2 << shift);
//            cnt++;
//            if(cnt == 3){
//                buffer.putLong(writeVal);
//                writeVal = 0;
//                shift = 0;
//                cnt = 0;
//            }else{
//                shift += 20;
//            }
//        }
//        if(cnt != 0){
//            buffer.putLong(writeVal);
//        }
//
//        buffer.flip();
//        return buffer;
//    }
//
//    static class DecoderBuffer{
//        long[] bufferLong;
//        int arrayIdx;
//        boolean highSlot;   // slot1 or slot3
//
//        DecoderBuffer(int slotNum){
//            bufferLong = new long[slotNum >> 1];
//            arrayIdx = 0;
//            highSlot = false;
//        }
//
//        public void append(long slotContent){
//            if(highSlot){
//                bufferLong[arrayIdx] |= (slotContent << 32);
//                arrayIdx++;
//                highSlot = false;
//            }else{
//                bufferLong[arrayIdx] |= slotContent;
//                highSlot = true;
//            }
//        }
//
//        public long[] getBufferLong(){
//            return bufferLong;
//        }
//    }
//
//    public static UpdatedMarkers deserialize(ByteBuffer buffer){
//        int slotNum = buffer.getInt();
//        DecoderBuffer db = new DecoderBuffer(slotNum);
//        for(int i = 0; i < slotNum; i = i + 3){
//            long val = buffer.getLong();
//            int remaining = slotNum - i;
//            switch (remaining){
//                case 1:
//                    db.append(val & 0xfffff);
//                    break;
//                case 2:
//                    db.append(val & 0xfffff);
//                    db.append((val >> 20) & 0xfffff);
//                    break;
//                default:
//                    // >= 3
//                    db.append(val & 0xfffff);
//                    db.append((val >> 20) & 0xfffff);
//                    db.append((val >> 40) & 0xfffff);
//                    break;
//            }
//        }
//        return new UpdatedMarkers(db.getBufferLong());
//    }
 */