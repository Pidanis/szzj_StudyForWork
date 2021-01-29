package com.aliyun.odps.examples.flink.transform;

import com.aliyun.odps.examples.flink.beans.sensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class functionRichFunctionDemo {

    // 自定义实现UDF
    public static class MyMapper implements MapFunction<sensorReading, Tuple2<String, Integer>>{

        @Override
        public Tuple2<String, Integer> map(sensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(), sensorReading.getId().length());
        }
    }

    // 自定义实现富函数UDF
    public static class MyRichMapper extends RichMapFunction<sensorReading, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(sensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(), getRuntimeContext().getIndexOfThisSubtask());// 子任务号 - 分区号
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作，一般是定义状态，或者建立数据库连接【防止每次数据来连接一次】
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            // 一般是关闭连接或情况状态收尾操作
            super.close();
        }
    }
}
