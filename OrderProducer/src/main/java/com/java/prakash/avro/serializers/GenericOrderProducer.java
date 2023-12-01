package com.java.prakash.avro.serializers;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class GenericOrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "\"namespace\" : \"com.java.prakash.avro\",\n" +
                "\"type\" : \"record\",\n" +
                "\"name\" : \"Order\",\n" +
                "\"fields\" : [\n" +
                "{\"name\" : \"customerName\" , \"type\" : \"string\"},\n" +
                "{\"name\" : \"product\" , \"type\" : \"string\"},\n" +
                "{\"name\" : \"qty\" , \"type\" : \"int\"}\n" +
                "]\n" +
                "}");
        GenericRecord order = new GenericData.Record(schema);
        order.put("customerName", "Prakash");
        order.put("product", "Iphone");
        order.put("qty", 100);
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("OrderAvroGRTopic", order.get("customerName").toString(), order);
        try {
            Future<RecordMetadata> future = producer.send(record);
            System.out.println("Message Sent Successfully to this topic : "+future.get().topic());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception : "+e.getMessage());
        }
    }
}
