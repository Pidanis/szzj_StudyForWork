package kafka_study.Consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class consumerSample {
    public final static String TopicName = "mysql-test-student";
    public final static long time = 1000;

    public static void main(String[] args) {

        helloWorld();
//        helloPartitions();
//        partitionOffset();

    }

    private static void helloWorld(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.60.61:9092"); //需要连接的kafka
        properties.put("group.id", "test-consumer-mysql3");

        properties.put("auto.commit.interval.ms", "100000");
        properties.put("max.poll.records",10);

        properties.put("enable.auto.commit", "true");
        //enable.auto.commit 控制是否提交到kafka、记录某个consumer消费到哪个offset
        //true/false true会提交 / false不会提交
        //注：kafka是通过consumer消费的offset记录来安排consumer能消费到的数据；enable.auto.commit来设置是否自动向kafka发送consumer消费到哪个offset


        properties.put("auto.offset.reset", "earliest");
        //auto.offset.reset 控制offset初始值
        //earliest/latest/none （这些相对值都是相对于kafka所在服务器保存的数据，如果需要之前的全部数据，需要）
        //earliest：当各分区已有已提交的offset时，从提交的offset开始消费；无提交的offset时(消费者刚初始化),从头开始
        //latest：当各分区已有已提交的offset时，从提交的offset开始消费；无提交时，之前的数据都不会被消费，只有新produce的数据才会被消费
        //none：topic各分区都存在已提交的offset时，从提交的offest处开始消费；只要有一个分区不存在已提交的offset，则抛出异常


        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);

        consumer.subscribe(Arrays.asList(TopicName, "Pidan")); //消费订阅哪一个Topic或者哪几个Topic
        try{
            while(true) {

//                consumer.seek(new TopicPartition(TopicName, 0), 26);

                ConsumerRecords<String, String> records = consumer.poll(time);//每1000ms拉取一次
                for (ConsumerRecord<String, String> record : records) {
//                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                    System.out.println(record);
                }
                consumer.commitAsync();
                //Async异步提交offset
                //这种情况是可能导致offset已经提交到3000，但才收到已提交完成2000的响应

    //                consumer.commitSync();
                //Sync同步提交offset
                //这种情况是阻塞的，需要future返回success
             }
            //备注：
            //一般采用的是正常异步
            //报错时候同步，确保offset被记录
        }catch (Exception e){
            consumer.commitSync();
        }

    }

    private static void partitionOffset(){
        Properties properties = new Properties();

        properties.put("bootstrap.servers", "192.168.60.61:9092");
        properties.put("group.id", "test2");
        properties.put("auto.commit.interval.ms", "100000");
        properties.put("max.poll.records",10);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        TopicPartition TP0 = new TopicPartition(TopicName, 0);
        TopicPartition TP1 = new TopicPartition("pidan", 0);

        consumer.assign(Arrays.asList(TP0, TP1));

        Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();

        try{
            while(true){
                consumer.seek(TP0, 26);
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for(ConsumerRecord record : records){
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
//                    currentOffset.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset(), "metadata"));
                }
            }
        }catch (Exception e){
//            consumer.commitSync(currentOffset);
        }

    }


    private static void helloPartitions(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.60.59:9092");
        properties.put("group.id", "");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(TopicName);
        for (PartitionInfo partitionifo: partitionInfos
             ) {
            System.out.println(partitionifo.partition());
        }
    }

}
