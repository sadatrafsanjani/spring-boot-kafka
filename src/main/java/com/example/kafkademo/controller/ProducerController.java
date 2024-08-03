package com.example.kafkademo.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping
@RequiredArgsConstructor
public class ProducerController {

    private final KafkaProducer<String, String> producer;

    @GetMapping
    public String producer(){

        String topic = "topic_1";
        String key = "aster_key";
        String value = "*_* *_* *_*";

        ProducerRecord<String, String> record = new ProducerRecord<>("aster", key, value);

        producer.send(record, (recordMetadata, e) -> {

            if(e == null){
                log.info("Topic: --- " + recordMetadata.topic());
                log.info("Partition: --- " + recordMetadata.partition());
                log.info("Offset: --- " + recordMetadata.offset());
                log.info("Timestamp: --- " + recordMetadata.timestamp());
            }
            else{
                log.debug(e.getMessage());
            }
        });

        producer.flush();
        //producer.close();

        return "Message published...";
    }

    @KafkaListener(topics = "aster", groupId = "groupId")
    public void consumer(String data){

        System.out.println(data + " ......... ");
    }
}
