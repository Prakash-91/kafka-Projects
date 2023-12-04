package com.prakash.kafka.controller;

import com.prakash.kafka.dto.User;
import com.prakash.kafka.service.UserProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/userapi")
public class UserController {

    @Autowired
    private UserProducerService userProducerService;

    /*@RequestMapping("/publishUserData/{name}/{age}")
    public void sendUserDate(@PathVariable("name") String name, @PathVariable("age") int age) {
        userProducerService.sendUserData(name, age);
    }*/

    @PostMapping("/publishUserData")
    public void sendUserDate(@RequestBody User user) {
        userProducerService.sendUserData(user);
    }
}
