package store;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

/*
Given the following sql
String createSQL = "create table CRIMES(" +
                    "       type VARCHAR(32)," +
                    "       id int," +
                    "       beat int," +
                    "       district int," +
                    "       latitude double," +
                    "       longitude double," +
                    "       eventTime long);";
we store records in fixed length style
 */

public class EventSchema {
    private String tableName;                               // table name
    private int fixedRecordLen;                             // record length
    private List<String> columnNames;                       // column names
    private List<DataType> dataTypes;                       // column data type
    private List<Integer> dataLengths;                      // column length
    // columnName -> basic information
    private Map<String, ColumnInfo> columnInfoMap = new HashMap<>(16);
    // this field help us to obtain timestamp faster
    @JSONField(serialize=false)
    private ColumnInfo eventTimeColInfo;

    // please note that after creating table, we will store schema into json file
    public EventSchema(String createTableStatement) {
        // example: "create table NASDAQ(ticker VARCHAR(8), price float, volume int, timestamp long);
        createTableStatement = createTableStatement.toUpperCase();
        Pattern tableNamePattern = Pattern.compile("CREATE\\s+TABLE\\s+(\\w+)\\s*\\(");
        Matcher tableNameMatcher = tableNamePattern.matcher(createTableStatement);

        if (tableNameMatcher.find()) {
            tableName = tableNameMatcher.group(1);
        }

        // read column name and column data type
        columnNames = new ArrayList<>(8);
        dataTypes = new ArrayList<>(8);
        dataLengths = new ArrayList<>(8);

        Pattern columnPattern = Pattern.compile("(\\w+)\\s+(\\w+\\(\\d+\\)|\\w+)(?=[,)])");
        Matcher columnMatcher = columnPattern.matcher(createTableStatement);

        fixedRecordLen = 0;
        while (columnMatcher.find()) {
            String columnName = columnMatcher.group(1);
            columnNames.add(columnName);
            String columnType = columnMatcher.group(2);
            if(columnType.equals("INT")) {
                dataTypes.add(DataType.INT);
                dataLengths.add(4);
                columnInfoMap.put(columnName, new ColumnInfo(fixedRecordLen, 4, DataType.INT));
                fixedRecordLen += 4;
            }else if(columnType.equals("LONG")){
                dataTypes.add(DataType.LONG);
                dataLengths.add(8);
                columnInfoMap.put(columnName, new ColumnInfo(fixedRecordLen, 8, DataType.LONG));
                fixedRecordLen += 8;
            }else if(columnType.equals("FLOAT")) {
                dataTypes.add(DataType.FLOAT);
                dataLengths.add(4);
                columnInfoMap.put(columnName, new ColumnInfo(fixedRecordLen, 4, DataType.FLOAT));
                fixedRecordLen += 4;
            }else if(columnType.equals("DOUBLE")) {
                dataTypes.add(DataType.DOUBLE);
                dataLengths.add(8);
                columnInfoMap.put(columnName, new ColumnInfo(fixedRecordLen, 8, DataType.DOUBLE));
                fixedRecordLen += 8;
            }else if(columnType.contains("VARCHAR")) {
                dataTypes.add(DataType.VARCHAR);
                String[] splits = columnType.split("[()]");
                int curLength = Integer.parseInt(splits[1]);
                dataLengths.add(curLength);
                columnInfoMap.put(columnName, new ColumnInfo(fixedRecordLen, curLength, DataType.VARCHAR));
                fixedRecordLen += curLength;
            }else{
                throw new RuntimeException("Unsupported column type: " + columnType);
            }
        }

        // here we need to check whether created sql has eventTime attribute
        if(!columnInfoMap.containsKey("EVENTTIME")){
            throw new RuntimeException("for events, create table must have eventTime attribute");
        }else{
            eventTimeColInfo = columnInfoMap.get("EVENTTIME");
        }

        // store this information into json file
        String jsonString = JSON.toJSONString(this);
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep + "java" + sep + "store" + sep;
        try (FileWriter fileWriter = new FileWriter(prefixPath + tableName + ".json")) {
            fileWriter.write(jsonString);
        } catch (Exception e) {
           System.out.println(e.getMessage());
        }
    }


    // we need this class
    @SuppressWarnings("unused")
    public EventSchema(String tableName, int fixedRecordLen, List<String> columnNames, List<DataType> dataTypes, List<Integer> dataLengths){
        this.tableName = tableName;
        this.fixedRecordLen = fixedRecordLen;
        this.columnNames = columnNames;
        this.dataTypes = dataTypes;
        this.dataLengths = dataLengths;

        columnInfoMap = new HashMap<>(10);
        int size = columnNames.size();
        int startPos = 0;
        for(int i = 0; i < size; i++){
            String columnName = columnNames.get(i);
            int dataLen = dataLengths.get(i);
            columnInfoMap.put(columnName, new ColumnInfo(startPos, dataLen, dataTypes.get(i)));
            startPos += dataLen;
        }

        eventTimeColInfo = columnInfoMap.get("EVENTTIME");
    }

    public static EventSchema getEventSchema(String tableName) {
        // tableName = tableName.toUpperCase();
        EventSchema schema = null;
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep + "java" + sep + "store" + sep;
        // change getLine()
        try (FileReader fileReader = new FileReader(prefixPath + tableName + ".json")) {
            // if json file is big, we need to modify 1024 to a big value
            char[] buffer = new char[1024];
            int numRead = fileReader.read(buffer);
            String jsonString = new String(buffer, 0, numRead);
            schema = JSON.parseObject(jsonString, EventSchema.class);
            return schema;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return schema;
    }

    public final DataType getDataType(String columnName) {
        return columnInfoMap.get(columnName).getDataType();
    }

    public byte[] covertStringToBytes(String record){
        // example: MSFT, 10.2, 100, 1010212
        String[] splits = record.split(",");
        int len = splits.length;

        if(len != columnNames.size()){
            throw new RuntimeException("Unsupported number of columns: " + len);
        }

        ByteBuffer buffer = ByteBuffer.allocate(fixedRecordLen);
        for(int i = 0; i < len; i++){
            switch(dataTypes.get(i)){
                case INT:
                    buffer.putInt(Integer.parseInt(splits[i]));
                    break;
                case LONG:
                    buffer.putLong(Long.parseLong(splits[i]));
                    break;
                case FLOAT:
                    buffer.putFloat(Float.parseFloat(splits[i]));
                    break;
                case DOUBLE:
                    buffer.putDouble(Double.parseDouble(splits[i]));
                    break;
                case VARCHAR:
                    byte[] bytes = splits[i].getBytes();
                    int varcharLen = dataLengths.get(i);
                    byte[] targetBytes = new byte[varcharLen];
                    System.arraycopy(bytes, 0, targetBytes, 0, Math.min(bytes.length, varcharLen));
                    buffer.put(targetBytes);
                    break;
                default:
                    throw new RuntimeException("Unsupported data type: " + dataTypes.get(i));
            }
        }
        buffer.flip();
        return buffer.array();
    }

    public byte[] batchConvertStringToBytes(String[] records){
        int recordNum = records.length;
        ByteBuffer buffer = ByteBuffer.allocate(fixedRecordLen * recordNum);

        for(String record : records){
            String[] splits = record.split(",");
            for(int i = 0; i < splits.length; i++){
                switch(dataTypes.get(i)){
                    case INT:
                        buffer.putInt(Integer.parseInt(splits[i]));
                        break;
                    case LONG:
                        buffer.putLong(Long.parseLong(splits[i]));
                        break;
                    case FLOAT:
                        buffer.putFloat(Float.parseFloat(splits[i]));
                        break;
                    case DOUBLE:
                        buffer.putDouble(Double.parseDouble(splits[i]));
                        break;
                    case VARCHAR:
                        byte[] bytes = splits[i].getBytes();
                        int varcharLen = dataLengths.get(i);
                        byte[] targetBytes = new byte[varcharLen];
                        System.arraycopy(bytes, 0, targetBytes, 0, Math.min(bytes.length, varcharLen));
                        buffer.put(targetBytes);
                        break;
                    default:
                        throw new RuntimeException("Unsupported data type: " + dataTypes.get(i));
                }
            }
        }

        buffer.flip();
        return buffer.array();
    }

    @Deprecated
    public Object getColumnValue(String columnName, MappedByteBuffer buffer, int pagePos){
        ColumnInfo columnInfo = columnInfoMap.get(columnName);
        //ColumnInfo columnInfo = columnInfoMap.get(columnName);
        DataType dataType = columnInfo.getDataType();
        int startPos = columnInfo.getStartPos();
        switch(dataType){
            case INT:
                return buffer.getInt(startPos + pagePos);
            case LONG:
                return buffer.getLong(startPos + pagePos);
            case FLOAT:
                return buffer.getFloat(startPos + pagePos);
            case DOUBLE:
                return buffer.getDouble(startPos + pagePos);
            case VARCHAR:
                // due to we use fix length to store this column
                // and string without end mark, we need to truncate this byte array
                int offset = columnInfo.getOffset();
                byte[] result = new byte[offset];
                buffer.position(startPos + pagePos);
                buffer.get(result, 0, offset);
                return result;
            default:
                throw new RuntimeException("Unsupported data type: " + dataType);
        }
    }

    // new version: faster than getColumnValue(columnName, buffer, pagePos)
    public Object getColumnValue(ColumnInfo columnInfo, ByteBuffer buffer, int pagePos){
        DataType dataType = columnInfo.getDataType();
        int startPos = columnInfo.getStartPos();
        switch(dataType){
            case INT:
                return buffer.getInt(startPos + pagePos);
            case LONG:
                return buffer.getLong(startPos + pagePos);
            case FLOAT:
                return buffer.getFloat(startPos + pagePos);
            case DOUBLE:
                return buffer.getDouble(startPos + pagePos);
            case VARCHAR:
                // due to we use fix length to store this column
                // and string without end mark, we need to truncate this byte array
                int offset = columnInfo.getOffset();
                byte[] result = new byte[offset];
                buffer.position(startPos + pagePos);
                buffer.get(result, 0, offset);
                return result;
            default:
                throw new RuntimeException("Unsupported data type: " + dataType);
        }
    }

    /**
     * note that all events have 'eventTime' column
     * @param record    byte record
     * @return          event's timestamp
     */
    public long getTimestamp(byte[] record){
        //ColumnInfo columnInfo = columnInfoMap.get("EVENTTIME");
        int startPos = eventTimeColInfo.getStartPos();
        int offset = eventTimeColInfo.getOffset();
        ByteBuffer buffer = ByteBuffer.wrap(record, startPos, offset);
        return buffer.getLong();
    }

    public ColumnInfo getColumnInfo(String columnName){
        return columnInfoMap.get(columnName);
    }

    public Object getColumnValue(String columnName, byte[] record){
        // this is for nfa
        if(columnName.equals("null")){
            return "null";
        }

        ColumnInfo columnInfo = columnInfoMap.get(columnName);
        int startPos = columnInfo.getStartPos();
        int offset = columnInfo.getOffset();
        ByteBuffer buffer = ByteBuffer.wrap(record, startPos, offset);

        switch(columnInfo.getDataType()){
            case INT:
                return buffer.getInt();
            case LONG:
                return buffer.getLong();
            case FLOAT:
                return buffer.getFloat();
            case DOUBLE:
                return buffer.getDouble();
            case VARCHAR:
                int idx;
                for(idx = startPos; idx < startPos + offset; ++idx){
                    // System.out.println("char: " + (char)(record[idx]));
                    if(record[idx] == 0){
                        break;
                    }
                }
                byte[] str = new byte[idx - startPos];
                buffer.get(str);
                return new String(str, 0, idx - startPos);
            default:
                throw new RuntimeException("Unsupported data type: " + columnInfo.getDataType());
        }
    }

    // this function is used by neq join
    // please call getColumnInfo to obtain columnInfo
    public int getValueId(ColumnInfo columnInfo, byte[] record, double minValue, double minRange){
        // maxValue - minValue = range
        int startPos = columnInfo.getStartPos();
        int offset = columnInfo.getOffset();
        ByteBuffer buffer = ByteBuffer.wrap(record, startPos, offset);
        switch(columnInfo.getDataType()){
            case INT:
                int intValue = buffer.getInt();
                return (int) ((intValue - minValue) / minRange);
            case LONG:
                long longValue = buffer.getLong();
                return (int) ((longValue - minValue) / minRange);
            case FLOAT:
                float floatValue = buffer.getFloat();
                return (int) ((floatValue - minValue) / minRange);
            case DOUBLE:
                double doubleValue = buffer.getDouble();
                return (int) ((doubleValue - minValue) / minRange);
            default:
                throw new RuntimeException("Unsupported data type: " + columnInfo.getDataType());
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getFixedRecordLen() {
        return fixedRecordLen;
    }

    public void setFixedRecordLen(int fixedRecordLen) {
        this.fixedRecordLen = fixedRecordLen;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(List<DataType> dataTypes) {
        this.dataTypes = dataTypes;
    }

    public List<Integer> getDataLengths() {
        return dataLengths;
    }

    public void setDataLengths(List<Integer> dataLengths) {
        this.dataLengths = dataLengths;
    }

    public String getRecordStr(byte[] record){
        ByteBuffer buffer = ByteBuffer.wrap(record);
        StringBuilder builder = new StringBuilder(128);
        int columnNum = columnNames.size();
        for(int i = 0; i < columnNum; i++){
            DataType dataType = dataTypes.get(i);
            switch(dataType){
                case INT:
                    builder.append(buffer.getInt());
                    break;
                case LONG:
                    builder.append(buffer.getLong());
                    break;
                case FLOAT:
                    builder.append(buffer.getFloat());
                    break;
                case DOUBLE:
                    builder.append(buffer.getDouble());
                    break;
                case VARCHAR:
                    int strLen = dataLengths.get(i);
                    byte[] result = new byte[strLen];
                    buffer.get(result);
                    int len = 0;
                    for (byte b : result) {
                        if (b == 0) {
                            break;
                        }
                        len++;
                    }
                    builder.append(new String(result, 0, len));
                    break;
                default:
                    throw new RuntimeException("Unsupported data type: " + dataType);
            }
            if(i != columnNum - 1){
                builder.append(",");
            }
        }
        return builder.toString();
    }

    public void print(){
        System.out.println("table name: " + tableName);
        for(int i=0; i<columnNames.size(); i++){
            System.out.println("column name: " + columnNames.get(i) + " type: " + dataTypes.get(i) + " len: " + dataLengths.get(i));
        }
        System.out.println(columnInfoMap);
    }

    // add new table
//    public static void main(String[] args) {
//        //String sql = "create table citibike(type VARCHAR(1), ride_id long, start_station_id int, end_station_id int, start_lat double, start_lng double, end_lat double, end_lng double, eventTime long);";
//        //String sql = "create table stock(id int, eventTime long, symbol int, price int, volume int)";
//        //String sql = "create table CRIMES(type VARCHAR(32), id int, beat int, district int, latitude double, longitude double, eventTime long);";
//
//        String sql = "create table synthetic(type VARCHAR(4), a1 int, a2 VARCHAR(8), a3 VARCHAR(16), eventTime long);";
//
//        EventSchema schema = new EventSchema(sql);
//
//        EventSchema.getEventSchema("SYNTHETIC").print();
//    }

}
