package kafka_study.Consumer;

//public class consumerConnector {
//}
import java.time.Duration;
        import java.util.Arrays;
        import java.util.Properties;

        import org.apache.kafka.clients.consumer.ConsumerRecord;
        import org.apache.kafka.clients.consumer.ConsumerRecords;
        import org.apache.kafka.clients.consumer.KafkaConsumer;

        import com.alibaba.fastjson.JSON;
        import com.alibaba.fastjson.JSONObject;

public class consumerConnector {
    public static void main(String[] args) {
        autoCommitConsumer();
    }

    /**
     * 自动提交
     */
    public static void autoCommitConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.60.61:9092");
        props.setProperty("group.id", "test3");
        props.setProperty("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("mysql-test-student"));
        //consumer.subscribe(Pattern.compile("sales-.*");
        System.out.println("开始接收消息。。。");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                // System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(),
                // record.key(), record.value());
                JSONObject jo = JSON.parseObject(record.value());
                String v = jo.getString("payload");
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), v);
            }
        }
    }

}

