package com.java.prakash.assignment.custom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class TruckSerializer implements Serializer<TruckCoordinate> {
    @Override
    public byte[] serialize(String topic, TruckCoordinate truckCoordinate) {
        byte[] response = null;
        ObjectMapper mapper = new ObjectMapper();
        try {
            response = mapper.writeValueAsString(truckCoordinate).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return response;
    }

    @Override
    public void close() {
    }
}
