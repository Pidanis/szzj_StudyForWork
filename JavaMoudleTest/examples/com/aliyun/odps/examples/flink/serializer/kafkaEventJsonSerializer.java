package com.aliyun.odps.examples.flink.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.aliyun.odps.examples.flink.beans.kafkaEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class kafkaEventJsonSerializer implements SerializationSchema<kafkaEvent>, DeserializationSchema<kafkaEvent> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(kafkaEvent kafkaEvent) {
        try{
            return mapper.writeValueAsBytes(kafkaEvent);
        }catch(JsonProcessingException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public kafkaEvent deserialize(byte[] bytes) throws IOException {
        return mapper.readValue(bytes, kafkaEvent.class);
    }

    @Override
    public boolean isEndOfStream(kafkaEvent kafkaEvent) {
        return false;
    }

    @Override
    public TypeInformation<kafkaEvent> getProducedType() {
        return TypeExtractor.getForClass(kafkaEvent.class);
    }
}
