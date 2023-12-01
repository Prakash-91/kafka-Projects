package com.java.prakash.assignment.custom;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TruckConsumer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", TruckDeserializer.class.getName());
        props.setProperty("group.id", "TruckGroup");

        KafkaConsumer<String, TruckCoordinate> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("TruckCSTopic"));
        ConsumerRecords<String, TruckCoordinate> consumerRecords = consumer.poll(Duration.ofSeconds(20));

        for (ConsumerRecord<String, TruckCoordinate> records : consumerRecords) {
            TruckCoordinate value = records.value();
            System.out.println("Truck Id :" + records.key());
            System.out.println("Truck Latitude :" + value.getLatitude());
            System.out.println("Truck Longitude :" + value.getLongitude());
        }
        consumer.close();

    }
}
