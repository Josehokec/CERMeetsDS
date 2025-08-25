package utils;

import java.util.Random;

public class MonteCarlo {


    public static void main(String[] args) {
        Random rand = new Random(11);

        double a = 1;
        double b = 2;
        double c = 6;

        int seedNum = 10000000;
        double overlappedLen = 0;
        for (int i = 0; i < seedNum; i++) {

            // [0, c - a]
            double aStart = rand.nextDouble() * (c - a);
            double aEnd = aStart + a;
            // [0, c - b]
            double bStart = rand.nextDouble() * (c - b);
            double bEnd = bStart + b;

            boolean nonOverlap = aEnd <= bStart || aStart >= bEnd;

            if(!nonOverlap) {
                overlappedLen = overlappedLen + Math.min(aEnd, bEnd) - Math.max(aStart, bStart);
            }
        }
        double avgOverlappedLen = overlappedLen / seedNum;
        System.out.printf("avgOverlappedLen: %f", avgOverlappedLen);


    }



}
