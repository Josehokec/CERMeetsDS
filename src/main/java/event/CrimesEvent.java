package event;

import store.EventSchema;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;


/*
Given the following sql
String createSQL = "create table CRIMES(" +
                    "       primary_type VARCHAR(32)," +
                    "       id int," +
                    "       beat int," +
                    "       district int," +
                    "       latitude double," +
                    "       longitude double," +
                    "       timestamp long);";
we store records in fixed length mode
 */
public class CrimesEvent{
    // please caution: these variable name should align with event schema
    public String type;
    public int id;
    public String caseNumber;
    public String IUCR;
    public int beat;
    public int district;
    public double latitude;
    public double longitude;
    public String FBICode;
    // flink cep need to define local data time rather long number
    public LocalDateTime eventTime;

    // this function is vital for flink cep
    public CrimesEvent(){}

    @Override
    public String toString(){
        return type + "," + id + "," + caseNumber + "," + IUCR + "," + beat + "," +
                district + "," + latitude + "," + longitude + "," + FBICode + "," + eventTime;
    }

    public CrimesEvent(String type, int id, String caseNumber, String IUCR, int beat, int district, double latitude, double longitude, String FBICode, LocalDateTime eventTime) {
        this.type = type;
        this.id = id;
        this.caseNumber = caseNumber;
        this.IUCR = IUCR;
        this.beat = beat;
        this.district = district;
        this.latitude = latitude;
        this.longitude = longitude;
        this.FBICode = FBICode;
        this.eventTime = eventTime;
    }

    public CrimesEvent(String line){
        //Type,ID,CaseNumber,IUCR,Beat,District,Latitude,Longitude,FBICode,EventTime
        //THEFT,12045583,JD226426,0820,212,2,41.830481843,-87.621751752,06,1588847040
        String[] splits = line.split(",");
        this.type = splits[0];
        this.id = Integer.parseInt(splits[1]);
        this.caseNumber = splits[2];
        this.IUCR = splits[3];
        this.beat = Integer.parseInt(splits[4]);;
        this.district = Integer.parseInt(splits[5]);;
        this.latitude = Double.parseDouble(splits[6]);
        this.longitude = Double.parseDouble(splits[7]);
        this.FBICode = splits[8];
        long ts = Long.parseLong(splits[9]);
        Instant instant = Instant.ofEpochMilli(ts * 1000);
        ZoneId zoneId = ZoneId.systemDefault();
        this.eventTime = LocalDateTime.ofInstant(instant, zoneId);
    }

    public static CrimesEvent valueOf(byte[] byteRecord){
        //{"columnNames":["TYPE","ID","CASENUMBER","IUCR","BEAT","DISTRICT","LATITUDE","LONGITUDE","FBICODE","EVENTTIME"],
        // "dataLengths":[24,4,9,4,4,4,8,8,3,8],
        // "dataTypes":["VARCHAR","INT","VARCHAR","VARCHAR","INT","INT","DOUBLE","DOUBLE","VARCHAR","LONG"]
        // "fixedRecordLen": 76

        int endPos = 24;
        for(int i = 0; i < 24; i++){
            if(byteRecord[i] == 0){
                endPos = i;
                break;
            }
        }

        String eventType = new String(byteRecord, 0, endPos);
        String caseNumber = new String(byteRecord, 28, 9);
        String IUCR = new String(byteRecord, 37, 4);
        String FBICode = new String(byteRecord, 65, 3);

        ByteBuffer buffer = ByteBuffer.wrap(byteRecord);

        int id = buffer.getInt(24);
        int beat = buffer.getInt(41);
        int district = buffer.getInt(45);;
        double latitude = buffer.getDouble(49);
        double longitude = buffer.getDouble(57);

        // s * 1000 = ms
        Instant instant = Instant.ofEpochMilli(buffer.getLong(68) * 1000);
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDateTime eventTime = LocalDateTime.ofInstant(instant, zoneId);

        return new CrimesEvent(eventType, id, caseNumber, IUCR, beat, district, latitude, longitude, FBICode, eventTime);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCaseNumber() {
        return caseNumber;
    }

    public void setCaseNumber(String caseNumber) {
        this.caseNumber = caseNumber;
    }

    public String getIUCR() {
        return IUCR;
    }

    public void setIUCR(String IUCR) {
        this.IUCR = IUCR;
    }

    public int getBeat() {
        return beat;
    }

    public void setBeat(int beat) {
        this.beat = beat;
    }

    public int getDistrict() {
        return district;
    }

    public void setDistrict(int district) {
        this.district = district;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getFBICode() {
        return FBICode;
    }

    public void setFBICode(String FBICode) {
        this.FBICode = FBICode;
    }

    public LocalDateTime getEventTime() {
        return eventTime;
    }

    public void setEventTime(LocalDateTime eventTime) {
        this.eventTime = eventTime;
    }
}
