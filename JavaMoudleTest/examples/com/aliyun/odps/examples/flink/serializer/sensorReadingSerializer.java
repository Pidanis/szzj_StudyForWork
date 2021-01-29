package com.aliyun.odps.examples.flink.serializer;

import com.aliyun.odps.examples.flink.beans.kafkaEvent;
import com.aliyun.odps.examples.flink.beans.sensorReading;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class sensorReadingSerializer implements SerializationSchema<sensorReading>, DeserializationSchema<sensorReading> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public sensorReading deserialize(byte[] bytes) throws IOException {
        return mapper.readValue(bytes, sensorReading.class);
    }

    @Override
    public boolean isEndOfStream(sensorReading sensorReading) {
        return false;
    }

    @Override
    public byte[] serialize(sensorReading sensorReading) {
        try{
            return mapper.writeValueAsBytes(sensorReading);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TypeInformation<sensorReading> getProducedType() {
        return TypeExtractor.getForClass(sensorReading.class);
    }
}
