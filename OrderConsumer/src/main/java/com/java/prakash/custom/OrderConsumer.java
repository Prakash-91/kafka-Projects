package com.java.prakash.custom;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class OrderConsumer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", OrderDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");
        //props.setProperty("auto.commit.interval.ms", "2000");
        props.setProperty("auto.commit.offset", "false"); // by default it is true

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);

        class RebalanceHandler implements ConsumerRebalanceListener {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                consumer.commitSync(currentOffsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {

            }
        }
        consumer.subscribe(Collections.singletonList("OrderPartionerTopic"));

        try {
            while (true) {
                ConsumerRecords<String, Order> consumerRecords = consumer.poll(Duration.ofSeconds(20));
                int count = 0;
                for (ConsumerRecord<String, Order> record : consumerRecords) {
                    String customerName = record.key();
                    Order order = record.value();
                    System.out.println("Customer Name : " + customerName);
                    System.out.println("Product Name : " + order.getProductName());
                    System.out.println("Quantity : " + order.getQty());
                    System.out.println("Partition : " + record.partition());

                    if (count % 10 == 0) {
                        Collections.singletonMap(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));
                        consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception ex) {
                                if (ex != null) {
                                    System.out.println("Commit Failed for offset :" + offsets);
                                }
                            }
                        });
                    }
                    count++;
                }
            }
        } finally {
            consumer.close();
        }
    }
}
