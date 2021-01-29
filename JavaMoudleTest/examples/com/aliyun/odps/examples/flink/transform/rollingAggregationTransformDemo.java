package com.aliyun.odps.examples.flink.transform;

import com.aliyun.odps.examples.flink.beans.sensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class rollingAggregationTransformDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStream = env.readTextFile("D:\\program_file\\IdeaProjects\\MaxComputerTest\\JavaMoudleTest\\examples\\com\\aliyun\\odps\\examples\\flink\\sensor.txt");

        // 转换成sensorreading类型
        SingleOutputStreamOperator<sensorReading> mapDataStream = dataStream.map(
//                new MapFunction<String, sensorReading>() {
//            @Override
//            public sensorReading map(String s) throws Exception {
//                String[] fields = s.split(",");
//                return new sensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//            }
//        }
                line -> {
                    String[] fields = line.split(", ");
                    return new sensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
                }
        );

        // 分组
        KeyedStream<sensorReading, Tuple> keyedStream = mapDataStream.keyBy("id");

//        KeyedStream<sensorReading, String> keyedStream1 = mapDataStream.keyBy(data -> data.getId());
//        KeyedStream<sensorReading, String> keyedStream1 = mapDataStream.keyBy(sensorReading :: getId);

        //滚动聚合
        SingleOutputStreamOperator<sensorReading> maxStream = keyedStream.max("temprature"); //其他字段（除keyBy、max字段），其他字段不会更新

        SingleOutputStreamOperator<sensorReading> maxByStream = keyedStream.maxBy("temprature"); //其他字段（除keyBy、max字段），也会跟随max字段更新


        maxStream.print();
        maxByStream.print();

        env.execute();
    }
}
