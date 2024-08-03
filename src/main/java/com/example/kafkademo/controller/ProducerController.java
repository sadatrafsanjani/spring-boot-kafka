package com.example.kafkademo.controller;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Random;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class ProducerController {

    private final KafkaTemplate<String , Object> kafkaTemplate;

    @GetMapping
    public String producer(){

        for(long i=1; i<=1000; i++){
            Random random = new Random();
            long number = random.nextLong(200 - 1) + 1;
            log.info("Number: {}", number);

            if(number/2 == 0){
                kafkaTemplate.send("even", String.valueOf(number));
                log.info("Want " + number + "nth Prime");
            }
            else {
                kafkaTemplate.send("odd", String.valueOf(number));
                log.info("Want " + number + "nth Prime");
            }
        }

        return "Message published...";
    }

    @KafkaListener(topics = "even", groupId = "groupId")
    public void consumeEven(@Payload String payload, Acknowledgment acknowledgment){

        int number = getNthPrime(Integer.parseInt(payload));

        if(number != 0){
            log.info(payload + "nth Prime = " + number);
            acknowledgment.acknowledge();
        }
    }

    @KafkaListener(topics = "odd", groupId = "groupId")
    public void consumerOdd(@Payload String payload, Acknowledgment acknowledgment){

        int number = getNthPrime(Integer.parseInt(payload));

        if(number != 0){
            log.info(payload + "nth Prime = " + number);
            acknowledgment.acknowledge();
        }
    }

    private int getNthPrime(int nth){

        log.info("Calculating " + nth + "nth Prime");

        int num, count, i;
        num=1;
        count=0;

        while (count < nth){
            num=num+1;
            for (i = 2; i <= num; i++){
                if (num % i == 0) {
                    break;
                }
            }
            if ( i == num){
                count++;
            }
        }

        return num;
    }
}
