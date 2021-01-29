package kafka_study.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

//再均衡监听器
public class rebalanceSample implements ConsumerRebalanceListener {
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {

    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

    }
}
