package kafka_study.Stream;

import com.google.gson.internal.Streams;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;


public class streamSample {

    //输入topic和输出topic
    //需要提前创建好
    private static final String InputTopic = "";
    private static final String InputTopic1 = "";
    private static final String OutputTopic = "";

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.60.59:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());//SERDE是Serizlize/Deserilize简称，目的是用于序列化和反序列化
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //构建流结构拓扑
        StreamsBuilder builder = new StreamsBuilder();
//        KStreamBuilder builder = builder;

        //KStream可以理解为一个抽象对象(栈)，从InputTopic不断抽取数据，不断压到KStream中
//        final KStream<Object, Object> stream = builder.stream(InputTopic, InputTopic1);
        final KStream<Object, Object> stream = builder.stream(Arrays.asList(InputTopic, InputTopic1));

        //table是数据集合的抽象对象
        KTable<String, Long> table = stream
                //flatMapValues相当于Hadoop的Map key：数据的其实位置 ； value：数据
                .flatMapValues(value -> Arrays.asList(value.toString().toLowerCase()))
                //groupBy相当于Hadoop中的partition
                .groupBy((key, value) -> value)
                //count相当于Hadoop中的reduce
                .count();

        //将结果输出到outputTopic
        table.toStream().to(OutputTopic);

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.start();

    }

}
