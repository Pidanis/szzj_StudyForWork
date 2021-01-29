package kafka_study.AdminClient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.internals.Topic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminSmaple {

    public static final String TopicName = "Pidan";

    public static void main(String[] args) throws Exception {
        //创建AdminClient
//        AdminClient adminClient = AdminSmaple.adminClient();
//        System.out.println("adminClient:" + adminClient);
        //创建Topics
        CreateTopic();
        //删除Topics
//        DeleteTopic(TopicName);
    }

    /*
        设置AdminClient
    */
    public static AdminClient adminClient(){
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.60.59:9092");

        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    /*
      创建topic
      会执行KafkaFuture，进行异步调用
      如果不调用get，异步调用还没有执行完，主线程就结束了，也可以在主线程中sleep一会
    */
    public static void CreateTopic() throws Exception {
        AdminClient adminClient = adminClient();
        short rp = 1;
        NewTopic topic = new NewTopic(TopicName, 1, rp);
        CreateTopicsResult topics =  adminClient.createTopics(Arrays.asList(topic));
        System.out.println("CreateTopicsResult:" + topics);

        //线程持久化 时间过了之后会消失topic
        //kafka-topic.sh --list --zookeeper localhost:2181 可查询到新的topic
//        Thread.sleep(100000);

        //代替线程持久化，将topic持久化 topic需要删除
        topics.all().get();
    }

    /*
        删除topic
     */
    public static void DeleteTopic(String topicname) throws Exception{
        AdminClient adminClient = adminClient();

        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(topicname));
        System.out.println("deleteTopicsResult:" + deleteTopicsResult);
//        deleteTopicsResult.all().get();
    }


    /*
        修改分区
     */
//    public static void AddPartition(String TopicName, Integer PartitionNum){
//        AdminClient adminClient = adminClient();
//        Map<String, NewPartitions> partitionsMap = new HashMap<>();
//        adminClient

//    }



}
