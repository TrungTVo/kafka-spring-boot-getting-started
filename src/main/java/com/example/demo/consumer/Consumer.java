package com.example.demo.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class Consumer {
    private final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @KafkaListener(
        id = "c1", 
        groupId = "myConsumer",
        topics = "hello",
        autoStartup = "false"
    )
    public void c1_myConsumer(String value,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        logger.info(String.format(
            "c1 consumed event from topic %s, partition %d, key = %-10s, value = %s",
            topic, partition, key, value));
    }


    @KafkaListener(
        id = "c2", 
        groupId = "myConsumer",
        topics = "hello",
        autoStartup = "false"
    )
    public void c2_myConsumer(String value,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        logger.info(String.format(
            "c2 consumed event from topic %s, partition %d, key = %-10s, value = %s",
            topic, partition, key, value));
    }


    // @KafkaListener(
    //     id = "c3", 
    //     groupId = "myConsumer",
    //     topics = "hello", 
    //     autoStartup = "false"
    // )
    // public void c3_myConsumer(String value,
    //         @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
    //         @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
    //         @Header(KafkaHeaders.RECEIVED_KEY) String key) {
    //     logger.info(String.format(
    //         "c3 consumed event from topic %s, partition %d, key = %-10s, value = %s",
    //         topic, partition, key, value));
    // }

}
