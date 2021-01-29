package com.aliyun.odps.examples.flink.table;

import com.aliyun.odps.examples.flink.beans.sensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class tableApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        DataStreamSource<String> dataStream = env.readTextFile("D:\\program_file\\IdeaProjects\\MaxComputerTest\\JavaMoudleTest\\examples\\com\\aliyun\\odps\\examples\\flink\\sensor.txt");

        // 转换为sensorReading类型
        SingleOutputStreamOperator<sensorReading> mapStream = dataStream.map(new MapFunction<String, sensorReading>() {

            @Override
            public sensorReading map(String s) throws Exception {
                String[] fields = s.split(", ");
                return new sensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        // 创建表环境
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        // 基于流创建一张表对象
        Table tableDate = tableEnv.fromDataStream(mapStream);

        // 调用tableAPI进行转换操作【对表对象操作】
        Table resultTable = tableDate.select("id, temprature")
                .where("id = 'sensor_1'");

        // 执行SQL
        tableEnv.registerTable("table1", tableDate); //注册表
        Table resultSQL = tableEnv.sqlQuery("select id, temprature from table1 where id = 'sensor_1'"); //对表操作

        tableEnv.toAppendStream(resultSQL, Row.class).print();

        env.execute();

    }
}
