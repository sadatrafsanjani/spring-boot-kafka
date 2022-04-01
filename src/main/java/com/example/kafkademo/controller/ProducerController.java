package com.example.kafkademo.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api")
public class ProducerController {

    private final KafkaTemplate<String , String> kafkaTemplate;

    @Autowired
    public ProducerController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping
    public String producer(){

        for (int i=1; i< 11; i++){
            kafkaTemplate.send("aster", "Hello World *_*");
        }

        return "Message published...";
    }

    @KafkaListener(topics = "aster", groupId = "groupId")
    public void consumer(String data){

        System.out.println(data + " ......... ");
    }
}
