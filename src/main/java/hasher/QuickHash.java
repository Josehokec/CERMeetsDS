package hasher;

//import java.util.Random;

public class QuickHash {
    //private static long previousSeed = 0;
    private static final long multiply_ = 0x9E3779B185EBCA87L;
    private static final long add_ = 0x165667B19E3779F9L;

//    public static long hash64(byte[] data) {
//        return hash64(data, data.length, 0);
//    }

//    public static void reset(long seed){
//        Random random = new Random(seed);
//        multiply_ = random.nextLong();
//        add_ = random.nextLong();
//        // set multiply_ & add_
//        for (int i = 0; i < 4; i++) {
//            multiply_ = (multiply_ << 32) | random.nextLong();
//            add_ = (add_ << 32) | random.nextLong();
//        }
//    }

//    public static long hash64(byte[] data, int length, long seed) {
//        long hashValue = 0;
//        if(seed == previousSeed){
//            for(int i = 0; i < length; i++){
//                hashValue = ((hashValue << 8) + (multiply_ * data[i]));
//            }
//            hashValue += add_;
//        }else{
//            reset(seed);
//            for(int i = 0; i < length; i++){
//                hashValue = ((hashValue << 8) + (multiply_ * data[i] + add_));
//            }
//            hashValue += add_;
//        }
//        return hashValue;
//    }

    // provide an interface to shrink filter
    public static long hash64(long key) {
        return key * multiply_ + add_;
    }

}
