package utils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class DemoTest {

    public static void main(String[] args) {
        Map<String, ByteBuffer> eventListMap = new HashMap<>();
        ByteBuffer buffer = ByteBuffer.allocate(1024 * 4);
        for (int i = 0; i < 1024; i++) {
            buffer.putInt(i);
        }
        buffer.flip();
        eventListMap.put("TEST", buffer);

        System.out.println(eventListMap.toString().length());
    }


}
