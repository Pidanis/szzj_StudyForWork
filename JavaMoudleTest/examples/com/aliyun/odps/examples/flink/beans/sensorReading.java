package com.aliyun.odps.examples.flink.beans;

public class sensorReading {
    private String id;
    private Long timestamp;
    private Double temprature;

    //空参构造方法
    public sensorReading() {
    }

    //全参构造方法
    public sensorReading(String id, Long timestamp, Double temprature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temprature = temprature;
    }
    //getter and setter javabean基本处理
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemprature() {
        return temprature;
    }

    public void setTemprature(Double temprature) {
        this.temprature = temprature;
    }

    //重写to_string利于序列化及print观察
    @Override
    public String toString() {
        return "sensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temprature=" + temprature +
                '}';
    }
}
