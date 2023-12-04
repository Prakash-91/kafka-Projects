package com.java.prakash;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {
    public static void main(String[] args) {

        Properties props = new Properties();
        /*props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty("group.id", "OrderGroup");*/

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OrderGroup");
        props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "10241234");
        props.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "200");
        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000");
        //30 partitions , 5 consumers , 6MB - 12 MB
        props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1MB");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "OrderConsumer");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());


        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderTopic"));
        ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofSeconds(20));

        for (ConsumerRecord<String, Integer> order : consumerRecords) {
            System.out.println("Product Name :" + order.key());
            System.out.println("Product Quantity :" + order.value());
        }
        consumer.close();

    }
}
