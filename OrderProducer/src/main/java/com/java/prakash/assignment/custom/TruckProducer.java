package com.java.prakash.assignment.custom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class TruckProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", TruckSerializer.class.getName());

        KafkaProducer<String, TruckCoordinate> producer = new KafkaProducer<>(props);
        TruckCoordinate coordinate = new TruckCoordinate();
        coordinate.setTruckNumber("TR-101");
        coordinate.setLatitude("22.5726 N");
        coordinate.setLongitude("88.3639 E");
        ProducerRecord<String, TruckCoordinate> truckDetails = new ProducerRecord<>("TruckCSTopic", coordinate.getTruckNumber(), coordinate);
        try {
            // This is a sync call
            Future<RecordMetadata> futureData = producer.send(truckDetails);
            System.out.println("Message Sent Successfully on this topic : "+futureData.get().topic());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
