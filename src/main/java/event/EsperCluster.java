package event;

import java.nio.ByteBuffer;

public class EsperCluster {
    private String type;
    private long JOBID;
    private int index;
    private String scheduling;
    private int priority;
    private float CPU;
    private float RAM;
    private float DISK;
    private long timestamp;

    public EsperCluster(String type, long JOBID, int index, String scheduling, int priority, float CPU, float RAM, float DISK, long timestamp) {
        this.type = type;
        this.JOBID = JOBID;
        this.index = index;
        this.scheduling = scheduling;
        this.priority = priority;
        this.CPU = CPU;
        this.RAM = RAM;
        this.DISK = DISK;
        this.timestamp = timestamp;
    }

    public static EsperCluster valueOf(byte[] byteRecord){
        String type = new String(byteRecord, 0, 3);
        String scheduling = new String(byteRecord, 15, 1);
        ByteBuffer buffer = ByteBuffer.wrap(byteRecord);
        long JOBID = buffer.getLong(3);
        int index = buffer.getInt(11);
        int priority = buffer.getInt(16);
        float CPU = buffer.getFloat(20);
        float RAM = buffer.getFloat(24);
        float DISK = buffer.getFloat(28);
        // us
        long timestamp = buffer.getLong(32);

        return new EsperCluster(type, JOBID, index, scheduling, priority, CPU, RAM, DISK, timestamp);
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString(){
        return type + "," + JOBID + "," + index + "," + scheduling + "," + priority + "," +
                CPU + "," + RAM + "," + DISK + "," + timestamp;
    }
}
