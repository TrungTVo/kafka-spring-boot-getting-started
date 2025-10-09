package com.example.demo.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import com.example.demo.models.Book;

@Service
public class BookProducer {
    private Logger logger = LoggerFactory.getLogger(BookProducer.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, Book> bookProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Book> bookKafkaTemplate() {
        return new KafkaTemplate<>(bookProducerFactory());
    }

    public void sendMessage(String topic, String messageKey, Book message) {
        CompletableFuture<SendResult<String, Book>> future = this.bookKafkaTemplate().send(topic, messageKey, message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                this.logger.info("Message sent successfully to topic " + topic + ": " + message.toString());
            } else {
                this.logger.error("Failed to send message to topic " + topic + ": " + ex.getMessage());
            }
        });
    }
}
