package kafka_study.Producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerSample {

    private static final String TopicName="Pidan";
    public static void main(String[] args) throws Exception {
        //producer异步发送
        producerSend();
    }
    /*
        异步发送演示
     */
    public static void producerSend() throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.60.59:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");


        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //Producer 主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        //ProducerRecoder 消息主对象
        for (int i = 10; i < 20; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TopicName, "key-"+i, "value-"+i);
            Future<RecordMetadata> send = producer.send(record);
            RecordMetadata recordMetadata = send.get();
            System.out.println("key" + i + " partition : " + recordMetadata.partition() + " offset : " + recordMetadata.offset());
        }

        //所有通道打开都必须关闭
        producer.close();
    }
}
