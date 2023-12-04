package com.java.prakash.custom.partioners;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class VIPPartioners implements Partitioner {

    @Override
    public void configure(Map<String, ?> map) {
        // Here we can configure ant addition thins wrt to partition
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        if(((String)key).equals("Prakash")){
            return 2;
        }
        return Math.abs((Utils.murmur2(keyBytes))%partitionInfos.size()-1);
    }

    @Override
    public void close() {
        // here resources can be closed
    }
}
