package com.prakash.kafka.service;

import com.prakash.kafka.dto.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class UserProducerService {

   /* @Autowired
    KafkaTemplate<String,Integer> kafkaTemplate;

    public void sendUserData(String name, int age){
        kafkaTemplate.send("user-topic",name,age);
    }*/

    @Autowired
    KafkaTemplate<String, User> kafkaTemplate;

    public void sendUserData(User user) {
        kafkaTemplate.send("user-topic", user.getName(), user);
    }
}
