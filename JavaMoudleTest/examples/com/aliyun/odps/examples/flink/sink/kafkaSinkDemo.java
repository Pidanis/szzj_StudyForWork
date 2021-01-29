package com.aliyun.odps.examples.flink.sink;

import com.aliyun.odps.examples.flink.beans.sensorReading;
import com.aliyun.odps.examples.flink.serializer.sensorReadingSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class kafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> datastream = env.readTextFile("D:\\program_file\\IdeaProjects\\MaxComputerTest\\JavaMoudleTest\\examples\\com\\aliyun\\odps\\examples\\flink\\sensor.txt");

        SingleOutputStreamOperator<sensorReading> mapStream = datastream.map(new MapFunction<String, sensorReading>() {
            @Override
            public sensorReading map(String s) throws Exception {
                String[] fields = s.split(", ");
                return new sensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        DataStreamSink<sensorReading> sensorReadingDataStreamSink = mapStream.addSink(new FlinkKafkaProducer<sensorReading>("192.168.60.61:9092", "sinkTest", new sensorReadingSerializer()));

        env.execute();
    }
}
