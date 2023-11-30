package com.java.prakash;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;

public class OrderCallBack implements Callback {

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println(recordMetadata.partition());
        System.out.println(recordMetadata.offset());
        System.out.println("Message Sent Successfully");

        if (null != e) {
            e.printStackTrace();
        }
    }
}
