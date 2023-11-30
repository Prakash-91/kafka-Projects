package com.java.prakash.assignment;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class TruckProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> truckDetails = new ProducerRecord<>("TruckTopic", "TR-101", "22.5726 N , 88.3639 E");
        try {
            // This is a sync call
            Future<RecordMetadata> futureData = producer.send(truckDetails);
            RecordMetadata recordMetadata = futureData.get();
            System.out.println("Message Sent Successfully");
            // This is Async call
            /*producer.send(truckDetails, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("Message Sent Successfully");
                    if (e != null) {
                        e.printStackTrace();
                    }
                }
            });*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
