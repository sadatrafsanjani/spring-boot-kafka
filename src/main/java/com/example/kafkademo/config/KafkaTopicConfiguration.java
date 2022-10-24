package com.example.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfiguration {

    @Bean
    public NewTopic topicConfiguration(){

        return TopicBuilder.name("aster").build();
    }

    @Bean
    public NewTopic topicConfigurationEven(){

        return TopicBuilder.name("even").build();
    }

    @Bean
    public NewTopic topicConfigurationOdd(){

        return TopicBuilder.name("odd").build();
    }

}
