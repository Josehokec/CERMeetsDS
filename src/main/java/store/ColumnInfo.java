package store;

public class ColumnInfo {
    public final int startPos;                    // byte array start position
    public final int offset;                      // storage length
    public final DataType dataType;               // data type

    public ColumnInfo(int startPos, int offset, DataType dataType) {
        this.startPos = startPos;
        this.offset = offset;
        this.dataType = dataType;
    }

    public int getStartPos() {
        return startPos;
    }

    public int getOffset() {
        return offset;
    }

    public DataType getDataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return "[startPos=" + startPos + ", offset=" + offset + ", " + dataType + "]";
    }
}
