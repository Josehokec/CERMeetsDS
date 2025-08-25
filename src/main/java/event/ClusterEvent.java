package event;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class ClusterEvent {
    public String type;
    public long JOBID;
    public int index;
    public String scheduling;
    public int priority;
    public float CPU;
    public float RAM;
    public float DISK;
    public LocalDateTime eventTime;

    public ClusterEvent(){}

    @Override
    public String toString(){
        return type + "," + JOBID + "," + index + "," + scheduling + "," + priority + "," +
                CPU + "," + RAM + "," + DISK + "," + eventTime;
    }

    public ClusterEvent(String type, long JOBID, int index, String scheduling, int priority,
                        float CPU, float RAM, float DISK, LocalDateTime eventTime) {
        this.type = type;
        this.JOBID = JOBID;
        this.index = index;
        this.scheduling = scheduling;
        this.priority = priority;
        this.CPU = CPU;
        this.RAM = RAM;
        this.DISK = DISK;
        this.eventTime = eventTime;
    }

    public ClusterEvent(String line){
        String[] fields = line.split(",");
        this.type = fields[0];
        this.JOBID = Long.parseLong(fields[1]);
        this.index = Integer.parseInt(fields[2]);
        this.scheduling = fields[3];
        this.priority = Integer.parseInt(fields[4]);
        this.CPU = Float.parseFloat(fields[5]);
        this.RAM = Float.parseFloat(fields[6]);
        this.DISK = Float.parseFloat(fields[7]);
        long ts = Long.parseLong(fields[8]);
        // eventTime[us]
        Instant instant = Instant.ofEpochMilli(ts/ 1000);
        ZoneId zoneId = ZoneId.systemDefault();
        this.eventTime = LocalDateTime.ofInstant(instant, zoneId);
    }

    public static ClusterEvent valueOf(byte[] byteRecord){
        String type = new String(byteRecord, 0, 3);
        String scheduling = new String(byteRecord, 15, 1);
        ByteBuffer buffer = ByteBuffer.wrap(byteRecord);
        long JOBID = buffer.getLong(3);
        int index = buffer.getInt(11);
        int priority = buffer.getInt(16);
        float CPU = buffer.getFloat(20);
        float RAM = buffer.getFloat(24);
        float DISK = buffer.getFloat(28);
        Instant instant = Instant.ofEpochMilli(buffer.getLong(32) / 1000);
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDateTime eventTime = LocalDateTime.ofInstant(instant, zoneId);
        return new ClusterEvent(type, JOBID, index, scheduling, priority, CPU, RAM, DISK, eventTime);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getJOBID() {
        return JOBID;
    }

    public void setJOBID(long JOBID) {
        this.JOBID = JOBID;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getScheduling() {
        return scheduling;
    }

    public void setScheduling(String scheduling) {
        this.scheduling = scheduling;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public float getCPU() {
        return CPU;
    }

    public void setCPU(float CPU) {
        this.CPU = CPU;
    }

    public float getRAM() {
        return RAM;
    }

    public void setRAM(float RAM) {
        this.RAM = RAM;
    }

    public float getDISK() {
        return DISK;
    }

    public void setDISK(float DISK) {
        this.DISK = DISK;
    }

    public LocalDateTime getEventTime() {
        return eventTime;
    }

    public void setEventTime(LocalDateTime eventTime) {
        this.eventTime = eventTime;
    }
}
