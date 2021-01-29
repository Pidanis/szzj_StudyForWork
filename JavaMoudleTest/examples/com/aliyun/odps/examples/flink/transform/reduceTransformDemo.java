package com.aliyun.odps.examples.flink.transform;

import com.aliyun.odps.examples.flink.beans.sensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class reduceTransformDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStream = env.readTextFile("D:\\program_file\\IdeaProjects\\MaxComputerTest\\JavaMoudleTest\\examples\\com\\aliyun\\odps\\examples\\flink\\sensor.txt");

        // 转换为sensorReading类型
        SingleOutputStreamOperator<sensorReading> mapStream = dataStream.map(new MapFunction<String, sensorReading>() {

            @Override
            public sensorReading map(String s) throws Exception {
                String[] fields = s.split(", ");
                return new sensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        // 分组
        KeyedStream<sensorReading, Tuple> keyByStream = mapStream.keyBy("id");

        // reduce聚合，取最大的温度值，以及当前最新的时间戳
        keyByStream.reduce(new ReduceFunction<sensorReading>() {
            @Override
            public sensorReading reduce(sensorReading sensorReading, sensorReading t1) throws Exception {
                return new sensorReading(sensorReading.getId(), t1.getTimestamp(), Math.max(sensorReading.getTemprature(), t1.getTemprature()));
            }
        });


        env.execute();
    }
}
