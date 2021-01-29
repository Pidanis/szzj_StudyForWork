package com.aliyun.odps.examples.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.connect.json.JsonDeserializer;


import java.util.Properties;

public class fromKafkaDemo {
    public static void main(String[] args) throws Exception{
        //环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka准备
        Properties pro = new Properties();
        pro.setProperty("bootstrap.servers", "192.168.60.61:9092");
        pro.setProperty("enable.auto.commit", "true");
        pro.setProperty("group.id", "fromKafkaDemo");
//        pro.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        pro.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pro.setProperty("auto.offset.reset", "earliest");

        //从kafka读取数据
        //addSource传入sourceFuntion类
        DataStreamSource<String> dataStream = env.addSource((new FlinkKafkaConsumer<String>("mysql-student", new SimpleStringSchema(), pro)));
//        FlinkKafkaConsumer011<String>("test1", new SimpleStringSchema(), pro)
        //打印
        dataStream.print();

        env.execute();
    }
}
