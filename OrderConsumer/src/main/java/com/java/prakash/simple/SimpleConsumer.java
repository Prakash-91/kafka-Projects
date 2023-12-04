package com.java.prakash.simple;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class SimpleConsumer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "SimpleConsumerGroup");

        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props);
        List<PartitionInfo> partitionInfos = consumer.partitionsFor("SimpleConsumerTopic");
        List<TopicPartition> partitions = new ArrayList<>();
        partitionInfos.stream().forEach(partitionInfo ->
                partitions.add(new TopicPartition("SimpleConsumerTopic", partitionInfo.partition())));
        consumer.assign(partitions);
        ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofSeconds(20));

        for (ConsumerRecord<String, Integer> order : consumerRecords) {
            System.out.println("Product Name :" + order.key());
            System.out.println("Product Quantity :" + order.value());
        }
        consumer.commitAsync();
        consumer.close();

    }
}
