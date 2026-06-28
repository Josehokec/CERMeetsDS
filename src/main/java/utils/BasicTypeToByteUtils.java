package utils;

import java.nio.ByteBuffer;

public class BasicTypeToByteUtils {
    public static byte[] intToBytes(int number){
        // same as: return ByteBuffer.allocate(4).putInt(number).array();
        byte[] b = new byte[4];
        b[3] = (byte) (number & 0xff);
        b[2] = (byte) (number >> 8 & 0xff);
        b[1] = (byte) (number >> 16 & 0xff);
        b[0] = (byte) (number >> 24 & 0xff);
        return b;
    }

    public static byte[] longToBytes(long number){
        // same as: return ByteBuffer.allocate(4).putInt(number).array();
        byte[] b = new byte[8];
        b[7] = (byte) (number & 0xff);
        b[6] = (byte) (number >> 8 & 0xff);
        b[5] = (byte) (number >> 16 & 0xff);
        b[4] = (byte) (number >> 24 & 0xff);
        b[3] = (byte) (number >> 32 & 0xff);
        b[2] = (byte) (number >> 40 & 0xff);
        b[1] = (byte) (number >> 48 & 0xff);
        b[0] = (byte) (number >> 56 & 0xff);
        return b;
    }

    public static byte[] floatToBytes(float number) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putFloat(number);
        return buffer.array();
    }

    @SuppressWarnings("unused")
    public static float bytesToFloat(byte[] bytes) {
        if (bytes.length != 4) {
            throw new IllegalArgumentException("Byte array must be of length 4");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.getFloat();
    }

    public static byte[] doubleToBytes(double number) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putDouble(number);
        return buffer.array();
    }

    @SuppressWarnings("unused")
    public static String byteToBinary(byte b) {
        StringBuilder binaryBuilder = new StringBuilder(16);
        for (int i = 7; i >= 0; i--) {
            binaryBuilder.append((b >> i) & 1);
        }
        return binaryBuilder.toString();
    }

    public static String longToBinary(long val){
        StringBuilder binaryBuilder = new StringBuilder(80);
        for (int i = 63; i >= 0; i--) {
            binaryBuilder.append((val >> i) & 1);
        }
        return binaryBuilder.toString();
    }

    public static String tagToBinary(int tag){
        StringBuilder binaryBuilder = new StringBuilder(24);
        for (int i = 15; i >= 0; i--) {
            binaryBuilder.append((tag >> i) & 1);
        }
        return binaryBuilder.toString();
    }
}