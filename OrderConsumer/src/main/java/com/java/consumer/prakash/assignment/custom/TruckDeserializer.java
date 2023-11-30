package com.java.consumer.prakash.assignment.custom;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class TruckDeserializer implements Deserializer<TruckCoordinate> {
    @Override
    public TruckCoordinate deserialize(String topic, byte[] bytes) {
        TruckCoordinate coordinate = null;
        ObjectMapper mapper = new ObjectMapper();
        try {
            coordinate = mapper.readValue(bytes, TruckCoordinate.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return coordinate;
    }
}
