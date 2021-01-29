package com.aliyun.odps.examples.flink.sink;

import com.aliyun.odps.examples.flink.beans.sensorReading;
import com.aliyun.odps.examples.flink.serializer.sensorReadingSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class mysqlSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> datastream = env.readTextFile("D:\\program_file\\IdeaProjects\\MaxComputerTest\\JavaMoudleTest\\examples\\com\\aliyun\\odps\\examples\\flink\\sensor.txt");

        SingleOutputStreamOperator<sensorReading> mapStream = datastream.map(new MapFunction<String, sensorReading>() {
            @Override
            public sensorReading map(String s) throws Exception {
                String[] fields = s.split(", ");
                return new sensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        mapStream.addSink(new MyJdbcSink());

        env.execute();
    }

    public static class MyJdbcSink extends RichSinkFunction<sensorReading> {
        // SinkFunction 中 invoke每来一个数据，调用一次
        // jdbc连接会过于频繁
        //根据生命周期，去创建连接，所以使用RichSinkFunction

        //生成连接
        Connection connection = null; //sql connector 全局变量
        //预编译语句
        PreparedStatement insert = null;
        PreparedStatement update = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 这里调用一个sql connector 会生成方法变量，在invoke中无法使用，所以需要创建全局变量
            connection = DriverManager.getConnection("jdbc:mysql://cdb-rub01ti8.cd.tencentcdb.com:10132/test", "root", "root123!");
            insert = connection.prepareStatement("insert into sensor (id, temperature) values (?, ?)");
            update = connection.prepareStatement("update sensor set temperature = ? where id = ?");
        }

        @Override
        public void invoke(sensorReading value, Context context) throws Exception {
            //每来一条数据，调用连接，执行sql
            // 直接执行更新语句，如果没有更新，那么就插入
            update.setDouble(1, value.getTemprature());
            update.setString(2, value.getId());
            update.execute();

            if(update.getUpdateCount() == 0){
                insert.setString(1, value.getId());
                insert.setDouble(2, value.getTemprature());
                insert.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insert.close();;
            update.close();
            connection.close();
        }
    }
}
