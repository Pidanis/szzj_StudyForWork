package com.aliyun.odps.examples.flink.beans;

import java.text.SimpleDateFormat;

public class kafkaEvent {
    private String eventType;
    private String imageName;
    private Long timestamp;
    private String instanceId;

    public kafkaEvent() {
    }

    public kafkaEvent(String eventType, String imageName, Long timestamp, String instanceId) {
        this.eventType = eventType;
        this.imageName = imageName;
        this.timestamp = timestamp;
        this.instanceId = instanceId;
    }

    @Override
    public int hashCode() {
        int result = eventType != null ? eventType.hashCode() : 0;
        result = 31 * result + (imageName != null ? imageName.hashCode() : 0);
        result = 31 * result + (int)(timestamp ^ (timestamp >>> 32));
        result = 31 * result + (instanceId != null ? instanceId.hashCode() : 0);

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj){
            return true;
        }else if(obj != null && getClass() == obj.getClass()){
            kafkaEvent that = (kafkaEvent) obj;
            return timestamp.equals(that.timestamp) && eventType.equals(that.eventType) && imageName.equals(that.imageName) && instanceId.equals(that.instanceId);
        } else{
            return false;
        }
    }

    @Override
    public String toString() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String formatTimestamp = format.format(timestamp);
        return "{\n" +
                "  \"eventType\": \"" + eventType + "\"\n," +
                "  \"imageName\": \"" + imageName + "\"\n," +
                "  \"timestamp\": \"" + formatTimestamp + "\"\n," +
                "  \"instanceId\": \"" + instanceId + "\"\n" +
                "}";
    }
}
