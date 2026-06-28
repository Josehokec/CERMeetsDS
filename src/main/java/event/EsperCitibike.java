package event;

import java.nio.ByteBuffer;

public class EsperCitibike {
    private String type;
    private long ride_id;
    private int start_station_id;
    private int end_station_id;
    private double start_lat;
    private double start_lng;
    private double end_lat;
    private double end_lng;
    private long timestamp;

    public EsperCitibike(String type, long ride_id, int start_station_id, int end_station_id, double start_lat, double start_lng, double end_lat, double end_lng, long timestamp) {
        this.type = type;
        this.ride_id = ride_id;
        this.start_station_id = start_station_id;
        this.end_station_id = end_station_id;
        this.start_lat = start_lat;
        this.start_lng = start_lng;
        this.end_lat = end_lat;
        this.end_lng = end_lng;
        this.timestamp = timestamp;
    }

    public static EsperCitibike valueOf(byte[] byteRecord){
        String eventType = new String(byteRecord, 0, 1);
        ByteBuffer buffer = ByteBuffer.wrap(byteRecord);
        long ride_id = buffer.getLong(1);
        int start_station_id = buffer.getInt(9);
        int end_station_id = buffer.getInt(13);
        double start_lat = buffer.getDouble(17);
        double start_lng = buffer.getDouble(25);
        double end_lat = buffer.getDouble(33);
        double end_lng = buffer.getDouble(41);
        long timestamp = buffer.getLong(49);
        return new EsperCitibike(eventType, ride_id, start_station_id, end_station_id,
                start_lat, start_lng, end_lat, end_lng, timestamp);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getRide_id() {
        return ride_id;
    }

    public void setRide_id(long ride_id) {
        this.ride_id = ride_id;
    }

    public int getStart_station_id() {
        return start_station_id;
    }

    public void setStart_station_id(int start_station_id) {
        this.start_station_id = start_station_id;
    }

    public int getEnd_station_id() {
        return end_station_id;
    }

    public void setEnd_station_id(int end_station_id) {
        this.end_station_id = end_station_id;
    }

    public double getStart_lat() {
        return start_lat;
    }

    public void setStart_lat(double start_lat) {
        this.start_lat = start_lat;
    }

    public double getStart_lng() {
        return start_lng;
    }

    public void setStart_lng(double start_lng) {
        this.start_lng = start_lng;
    }

    public double getEnd_lat() {
        return end_lat;
    }

    public void setEnd_lat(double end_lat) {
        this.end_lat = end_lat;
    }

    public double getEnd_lng() {
        return end_lng;
    }

    public void setEnd_lng(double end_lng) {
        this.end_lng = end_lng;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString(){
        return type + ", " + ride_id + ", " + start_station_id + ", " +
                end_station_id + ", " + start_lat + ", " + start_lng + ", " +
                end_lat + ", " + end_lng + ", " + timestamp;
    }
}
