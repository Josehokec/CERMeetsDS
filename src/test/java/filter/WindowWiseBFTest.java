package filter;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class WindowWiseBFTest {

    @Test
    public void test() {
        int seed = 7;
        int testKeyNum = 10000;

        // StandardBF vs. WindowWiseBF
        BasicBF bf = new WindowWiseBF(0.01, testKeyNum);
        for (int i = 0; i < testKeyNum; i++) {
            String key = "key" + i;
            bf.insert(key, seed);
        }

        ByteBuffer buffer = bf.serialize();
        buffer.rewind();

        // StandardBF vs. WindowWiseBF
        BasicBF bf2 = new WindowWiseBF(buffer);

        int fp = 0;
        for (int i = 0; i < testKeyNum * 11; i++) {
            String key = "key" + i;
            if(i < testKeyNum){
                Assert.assertTrue(bf.contain(key, seed));
            }else{
                if(bf2.contain(key, seed)){
                    fp++;
                }
            }
        }

        double fpr = (double) fp / (testKeyNum * 10) * 100;
        String formatted = String.format("%.4f", fpr);
        System.out.println("fpr: " + formatted + "%");
    }
}