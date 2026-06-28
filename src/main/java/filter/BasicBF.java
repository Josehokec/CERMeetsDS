package filter;

import java.nio.ByteBuffer;

public interface BasicBF {
    public void insert(String key, long windowId);
    public boolean contain(String key, long windowId);
    public ByteBuffer serialize();
}
