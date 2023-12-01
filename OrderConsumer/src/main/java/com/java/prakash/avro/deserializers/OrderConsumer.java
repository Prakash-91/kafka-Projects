package com.java.prakash.avro.deserializers;

import com.java.prakash.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");
        props.setProperty("schema.registry.url", "http://localhost:8081");
        props.setProperty("specific.avro.reader", "true");


        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderAvroTopic"));
        ConsumerRecords<String, Order> consumerRecords = consumer.poll(Duration.ofSeconds(20));

        for (ConsumerRecord<String, Order> record : consumerRecords) {
            String customerName = record.key();
            Order order = record.value();
            System.out.println("Customer Name : " + customerName);
            System.out.println("Product Name : " + order.getProduct());
            System.out.println("Quantity : " + order.getQty());
        }
        consumer.close();

    }
}
