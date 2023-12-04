package com.java.prakash.custom;

import com.java.prakash.custom.partioners.VIPPartioners;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "com.java.prakash.custom.OrderSerializer");
        props.setProperty("partitioner.class", VIPPartioners.class.getName());


        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        Order order = new Order();
        order.setCustomerName("Prakash");
        order.setProductName("Iphone");
        order.setQty(10);
        ProducerRecord<String, Order> record = new ProducerRecord<>("OrderPartionerTopic", order.getCustomerName(), order);
        try {
            // This is a fire and forget call
            Future<RecordMetadata> future = producer.send(record);
            System.out.println("Message Sent Successfully to this topic : "+future.get().topic());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
