package s3;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class DemoEvent{
    public String name;
    public LocalDateTime eventTime;
    public int sales;
    public String product_id;
    public String region;

    public DemoEvent(){}

    public DemoEvent(String name, long eventTime, int sales, String product_id, String region){
        this.name = name;
        this.sales = sales;
        this.product_id = product_id;
        this.region = region;
        this.eventTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(eventTime), ZoneId.systemDefault());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LocalDateTime getEventTime() {
        return eventTime;
    }

    public void setEventTime(LocalDateTime eventTime) {
        this.eventTime = eventTime;
    }

    public int getSales() {
        return sales;
    }

    public void setSales(int sales) {
        this.sales = sales;
    }

    public String getProduct_id() {
        return product_id;
    }

    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }
}