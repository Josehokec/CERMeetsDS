package store;

public class RID {
    private final int page;
    private final short offset;

    public RID(int page, short offset) {
        this.page = page;
        this.offset = offset;
    }

    public int getPage() {
        return page;
    }

    public short getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "page: " + page + " offset: " + offset;
    }
}
