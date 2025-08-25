package event;

import java.nio.ByteBuffer;

/**
 * this class is used for esper
 */
public class EsperCrimes {
    private String type;                // 0
    private int id;                     // 24
    private String caseNumber;          // 28
    private String IUCR;                // 37
    private int beat;                   // 41
    private int district;               // 45
    private double latitude;            // 49
    private double longitude;           // 57
    private String FBICode;             // 65
    private long timestamp;             // 68

    public EsperCrimes(String type, int id, String caseNumber, String IUCR, int beat, int district, double latitude, double longitude, String FBICode, long timestamp) {
        this.type = type;
        this.id = id;
        this.caseNumber = caseNumber;
        this.IUCR = IUCR;
        this.beat = beat;
        this.district = district;
        this.latitude = latitude;
        this.longitude = longitude;
        this.FBICode = FBICode;
        this.timestamp = timestamp;
    }

    public static EsperCrimes valueOf(byte[] byteRecord){
        // [24,4,9,4,4,4,8,8,3,8]
        String type = new String(byteRecord, 0, 24);
        int nullPos = 24;
        for(int i = 0; i < 24; i++){
            if(type.charAt(i) == 0){
                nullPos = i;
                break;
            }
        }
        type = type.substring(0, nullPos);

        String caseNumber = new String(byteRecord, 28, 9);
        nullPos = 0;
        for(int i = 0; i < 9; i++){
            if(caseNumber.charAt(i) == 0){
                nullPos = i;
                break;
            }
        }
        caseNumber = caseNumber.substring(0, nullPos);

        String IUCR = new String(byteRecord, 37, 4);
        String FBICode = new String(byteRecord, 65, 3);

        ByteBuffer buffer = ByteBuffer.wrap(byteRecord);
        int id = buffer.getInt(24);
        int beat = buffer.getInt(41);
        int district = buffer.getInt(45);
        double latitude = buffer.getDouble(49);
        double longitude = buffer.getDouble(57);
        long timestamp = buffer.getLong(68) * 1000;

        return new EsperCrimes(type, id, caseNumber, IUCR, beat, district, latitude, longitude, FBICode, timestamp);
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString(){
        return type + "," + id + "," + caseNumber + "," + IUCR + "," + beat + "," +
                district + "," + latitude + "," + longitude + "," + FBICode + "," + timestamp;
    }
}
