package com.aliyun.odps.examples.flink.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class basicTransformDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        DataStreamSource<String> dataStream = env.readTextFile("D:\\program_file\\IdeaProjects\\MaxComputerTest\\JavaMoudleTest\\examples\\com\\aliyun\\odps\\examples\\flink\\sensor.txt");
        
        // 1.map 把String转换为长度输出
        SingleOutputStreamOperator<Integer> mapDataStream = dataStream.map(new MapFunction<String, Integer>() {//String - 调用函数dataStream数据类型， Object输出dataStream数据类型
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }

        });

        // 2.flatmap 按逗号切分字段
        SingleOutputStreamOperator<String> flatMapDataStream = dataStream.flatMap(new FlatMapFunction<String, String>() {//String - 调用函数dataStream数据类型， Object输出dataStream数据类型[前一个，后一个]

            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] fields = s.split(",");
                for (String field : fields) {
                    collector.collect(field);
                }

            }
        });

        // 3. filter 筛选sensor_1开头的数据
        SingleOutputStreamOperator<String> fliterDataStream = dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor_1"); // prefix -- 前缀 toffset -- 字符串中开始查找的位置(默认为0)


            }
        });

//        System.out.println("mapDataStream:");
//        mapDataStream.print();

//        System.out.println("flatMapDataStream");
        flatMapDataStream.print();

//        System.out.println("filterDataStream");
//        fliterDataStream.print();

        env.execute();


    }

}
