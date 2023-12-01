package com.java.prakash.assignment;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TruckConsumer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("group.id", "TruckGroup");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("TruckTopic"));
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(20));

        for (ConsumerRecord<String, String> truckDetail : consumerRecords) {
            System.out.println("Truck Id :" + truckDetail.key());
            String[] truck = truckDetail.value().split(",");
            if(truck != null && truck.length > 0) {
                System.out.println("Truck Latitude :" + truck[0]);
                System.out.println("Truck Longitude :" + truck[1]);
            }
        }
        consumer.close();

    }
}
