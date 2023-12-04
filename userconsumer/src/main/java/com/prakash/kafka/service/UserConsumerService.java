package com.prakash.kafka.service;

import com.prakash.kafka.dto.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class UserConsumerService {

    /*@KafkaListener(topics = {"user-topic"})
    public void consumeUserData(int age){
        System.out.println("User Age is : "+age);
    }*/

    @KafkaListener(topics = {"user-topic"})
    public void consumeUserData(User user){
        System.out.println("User Age is : "+user.getAge()+" and Favourite Channel is : "+user.getFavGenre());
    }
}
