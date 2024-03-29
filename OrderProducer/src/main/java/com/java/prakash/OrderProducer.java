package com.java.prakash;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        /*props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");*/

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "343434343");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "2");
        props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "400");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32345678912");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "500");
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "200");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");


        KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "Mac Book Pro", 10);
        try {
            // This is a sync call
            Future<RecordMetadata> futureData = producer.send(record);
            RecordMetadata recordMetadata = futureData.get();
            System.out.println(recordMetadata.partition());
            System.out.println(recordMetadata.offset());
            System.out.println("Message Sent Successfully");

            // This is Async call
            //producer.send(record, new OrderCallBack());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
