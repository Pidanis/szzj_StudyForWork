package com.aliyun.odps.examples.flink.window;

import com.aliyun.odps.examples.flink.beans.sensorReading;
import com.aliyun.odps.io.Tuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class processWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> dataStream = env.readTextFile("D:\\program_file\\IdeaProjects\\MaxComputerTest\\JavaMoudleTest\\examples\\com\\aliyun\\odps\\examples\\flink\\sensor.txt");

        SingleOutputStreamOperator<sensorReading> mapStream = dataStream.map(new MapFunction<String, sensorReading>() {
            @Override
            public sensorReading map(String s) throws Exception {
                String[] fields = s.split(", ");
                return new sensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        mapStream.keyBy("id");
//                .process(new ProcessWindowFunction<>())
//                .window(new ProcessWindowFunction<sensorReading, Integer, Tuple, GlobalWindow>(){
//                    @Override
//                    public void process(Tuple tuple, Context context, Iterable<sensorReading> iterable, Collector<Integer> collector) throws Exception {
//                        int count = 0;
//                        for (sensorReading s_ : iterable){
//                            count++;
//                        }
//                        collector.collect(count);
//                    }
//
////                    @Override
////                    public void process(String s, Context context, Iterable<sensorReading> iterable, Collector<Integer> collector) throws Exception {
////                        int count = 0;
////                        for (sensorReading s_ : iterable){
////                            count++;
////                        }
////                        collector.collect(count);
////                    }
//                });

        env.execute();
    }
}
