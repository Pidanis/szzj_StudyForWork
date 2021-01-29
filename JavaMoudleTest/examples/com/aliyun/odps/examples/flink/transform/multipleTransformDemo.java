package com.aliyun.odps.examples.flink.transform;

import com.aliyun.odps.examples.flink.beans.sensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

public class multipleTransformDemo {
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

        // 1.按照30摄氏度分成两条流
        SplitStream<sensorReading> splitStream = mapStream.split(new OutputSelector<sensorReading>() {
            @Override
            public Iterable<String> select(sensorReading sensorReading) {
                return (sensorReading.getTemprature() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<sensorReading> selectHighStream = splitStream.select("high");
        DataStream<sensorReading> selectLowStream = splitStream.select("low");
        DataStream<sensorReading> selectAllStream = splitStream.select("high", "low");

        selectHighStream.print();
        selectLowStream.print();
        selectAllStream.print();

        // 2.合流 connect，将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息【connect 只能两条流】
        SingleOutputStreamOperator<Tuple2<String, Double>> warningStream = selectHighStream.map(new MapFunction<sensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(sensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemprature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, sensorReading> connectStream = warningStream.connect(selectLowStream); //只能两条流

        SingleOutputStreamOperator<Object> resultStream = connectStream.map(new CoMapFunction<Tuple2<String, Double>, sensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return new Tuple3<>(stringDoubleTuple2.f0, stringDoubleTuple2.f1, "high");
            }

            @Override
            public Object map2(sensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), "normal");
            }
        });

        resultStream.print();

        // 3.union联合多条流 【union 可以多条流 但是流的数据类型格式必须一致】
        DataStream<sensorReading> unionStream = selectHighStream.union(selectLowStream, selectAllStream);

        unionStream.print();

        env.execute();
    }
}
