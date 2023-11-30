package com.java.prakash.custom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class OrderSerializer implements Serializer<Order> {
    @Override
    public byte[] serialize(String topic, Order order) {
        byte[] response = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            System.out.println("Serializing ....");
            response = objectMapper.writeValueAsString(order).getBytes();
            System.out.println("Serialize and converting Order object to byte[] successfully");
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return response;
    }

    @Override
    public void close() {
    }
}
