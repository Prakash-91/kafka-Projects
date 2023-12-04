package com.java.prakash;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class TransactionalOrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-producer-1");
        //props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");


        KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "Mac Book Pro", 10);
        ProducerRecord<String, Integer> record2 = new ProducerRecord<>("OrderTopic", "Dell Pro", 20);
        try {
            producer.beginTransaction();
            Future<RecordMetadata> futureData = producer.send(record);
            RecordMetadata recordMetadata = futureData.get();
            Future<RecordMetadata> futureData2 = producer.send(record2);
            RecordMetadata recordMetadata2 = futureData.get();
            System.out.println("Message Sent Successfully");
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
