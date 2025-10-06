package com.example.demo.producer;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class Producer {
    private Logger logger = LoggerFactory.getLogger(Producer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String topic, String messageKey, Object message) {
        CompletableFuture<SendResult<String, String>> future = this.kafkaTemplate.send(topic, messageKey, message.toString());
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                this.logger.info("Message sent successfully to topic " + topic + ": " + message.toString());
            } else {
                this.logger.error("Failed to send message to topic " + topic + ": " + ex.getMessage());
            }
        });
    }
}
