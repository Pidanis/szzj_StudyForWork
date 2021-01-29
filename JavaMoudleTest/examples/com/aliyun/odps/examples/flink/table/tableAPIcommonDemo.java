package com.aliyun.odps.examples.flink.table;

import com.aliyun.odps.graph.DataType;
import org.apache.calcite.schema.Table;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.types.Row;
import scala.Option;

public class tableAPIcommonDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        // 表的创建
        // 1.读取文件
        String filePath = "D:\\program_file\\IdeaProjects\\MaxComputerTest\\JavaMoudleTest\\examples\\com\\aliyun\\odps\\examples\\flink\\sensor.txt";
        StreamTableDescriptor streamTableDescriptor = tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema().field("id", TypeInformation.of(String.class))
                        .field("timestamp", TypeInformation.of(Long.class))
                        .field("temp", TypeInformation.of(Double.class)));


    }
}
