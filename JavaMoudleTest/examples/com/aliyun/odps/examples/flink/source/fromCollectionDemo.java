package com.aliyun.odps.examples.flink.source;

import com.aliyun.odps.examples.flink.beans.sensorReading;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class fromCollectionDemo {
    public static void main(String[] args) throws Exception{
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        env.setParallelism(1);

        //从集合中读取数据
        DataStreamSource<sensorReading> dataStream = env.fromCollection(Arrays.asList(new sensorReading("sensor_1", 1547718199L, 35.8),
                new sensorReading("sensor_6", 1547718201L, 15.4),
                new sensorReading("sensor_7", 1547718202L, 6.7),
                new sensorReading("sensor_10", 1547718205L, 38.1)));

        //读取元素，也会产生dataStream
        DataStreamSource<Integer> dataStreamElements = env.fromElements(1, 2, 3, 4, 5);

        //打印输出
        dataStream.print().setParallelism(1);
        dataStreamElements.print().setParallelism(1);

        env.execute("初测试");
    }
}
