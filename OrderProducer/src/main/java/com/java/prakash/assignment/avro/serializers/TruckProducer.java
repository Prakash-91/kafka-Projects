package com.java.prakash.assignment.avro.serializers;

import com.java.prakash.assignment.avro.TruckCoordinate;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class TruckProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, TruckCoordinate> producer = new KafkaProducer<>(props);
        TruckCoordinate coordinate = new TruckCoordinate("TR-101","22.5726 N","88.3639 E");
        ProducerRecord<String, TruckCoordinate> truckDetails = new ProducerRecord<>("TruckAvroCSTopic", coordinate.getTruckNumber().toString(), coordinate);
        try {
            // This is a sync call
            Future<RecordMetadata> futureData = producer.send(truckDetails);
            System.out.println("Message Sent Successfully on this topic : "+futureData.get().topic());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
