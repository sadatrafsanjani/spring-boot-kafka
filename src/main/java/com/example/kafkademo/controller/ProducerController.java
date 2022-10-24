package com.example.kafkademo.controller;

import lombok.AllArgsConstructor;
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
@AllArgsConstructor
public class ProducerController {

    private final KafkaTemplate<String , String> kafkaTemplate;

    @GetMapping
    public String producer(){

        for(int i=1; i<=21; i++ ){
            Random random = new Random();
            int number = random.nextInt(200 - 1) + 1;
            kafkaTemplate.send("aster", String.valueOf(number));
            log.info("Want " + number + "nth Prime");

            if((i % 5) == 0){
                try{
                    Thread.sleep(2000);
                }
                catch (Exception e){}
            }
        }

        return "Message published...";
    }

    @KafkaListener(topics = "aster", groupId = "groupId")
    public void consumer(@Payload String payload, Acknowledgment acknowledgment){

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
