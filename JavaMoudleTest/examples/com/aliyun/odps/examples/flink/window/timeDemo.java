package com.aliyun.odps.examples.flink.window;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

public class timeDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        DataStreamSource<String> dataStream = env.readTextFile("D:\\program_file\\IdeaProjects\\MaxComputerTest\\JavaMoudleTest\\examples\\com\\aliyun\\odps\\examples\\flink\\sensor.txt");

        SingleOutputStreamOperator<String> timeDataStream = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String s) {
                String[] fields = s.split(", ");
                return Long.parseLong(fields[1]);
            }
        });

        SingleOutputStreamOperator<String> watermarksDataStream = dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

            private Long bound = 15 * 1000L;
            private Long maxTs = Long.MIN_VALUE;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(maxTs - bound);
            }

            @Override
            public long extractTimestamp(String s, long l) { // s:current event l:previous event-time
                String[] fields = s.split(", ");
                maxTs = Math.max(maxTs, Long.parseLong(fields[1]));
                return Long.parseLong(fields[1]);
            }
        });

//        dataStream.print();

        timeDataStream.print();

        env.execute();
    }
}
