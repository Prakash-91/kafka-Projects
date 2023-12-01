package com.java.prakash.avro.serializers;

import com.java.prakash.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        Order order = new Order("Prakash", "Iphone", 10);
        ProducerRecord<String, Order> record = new ProducerRecord<>("OrderAvroTopic", order.getCustomerName().toString(), order);
        try {
            Future<RecordMetadata> future = producer.send(record);
            System.out.println("Message Sent Successfully to this topic : " + future.get().topic());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
