package kafka_study.Producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class PartitionSample implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        /*
        s = topic;
        o = key;
        bytes = keyBytes;
        o1 = value;
        bytes1 = valueBytes;
         */

        String keyStr = o + "";
        String keyInt = keyStr.substring(4);

        int i = Integer.parseInt(keyInt);
        return i%2;
//        return 0;

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
