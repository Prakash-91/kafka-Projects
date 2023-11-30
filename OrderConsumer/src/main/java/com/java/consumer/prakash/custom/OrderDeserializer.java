package com.java.consumer.prakash.custom;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class OrderDeserializer implements Deserializer<Order> {

    @Override
    public Order deserialize(String topic, byte[] data) {
        Order order = null;
        if (data == null){
            System.out.println("Null received at deserializing");
            return order;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            System.out.println("Deserializing ...");
            order = objectMapper.readValue(data, Order.class);
            System.out.println("Deserializing byte[] to Order Object successfully");
        } catch (IOException e) {
            e.printStackTrace();
            throw new SerializationException("Error when deserializing byte[] to Order");
        }
        return order;
    }
}
