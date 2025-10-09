package com.example.demo.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.example.demo.models.Book;

@Service
public class BookConsumer {
    private final Logger logger = LoggerFactory.getLogger(BookConsumer.class);

    @KafkaListener(
        id = "c3", 
        groupId = "myConsumer",
        topics = "books",
        autoStartup = "false",
        containerFactory = "bookKafkaListenerContainerFactory"
    )
    public void c3_myConsumer(Book value,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        logger.info(String.format(
            "c3 consumed event from topic %s, partition %d, key = %-10s, value = %s",
            topic, partition, key, value.toString()));
    }

}
