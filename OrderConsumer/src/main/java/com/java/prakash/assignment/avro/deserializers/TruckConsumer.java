package com.java.prakash.assignment.avro.deserializers;

import com.java.prakash.assignment.avro.TruckCoordinate;
import com.java.prakash.assignment.custom.TruckDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
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
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("group.id", "TruckGroup");
        props.setProperty("schema.registry.url", "http://localhost:8081");
        props.setProperty("specific.avro.reader", "true");

        KafkaConsumer<String, TruckCoordinate> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("TruckAvroCSTopic"));
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
