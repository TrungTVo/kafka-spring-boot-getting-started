package com.example.demo.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import com.example.demo.Payment;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

@Service
public class TransactionProducer {
    private Logger logger = LoggerFactory.getLogger(TransactionProducer.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.schema-registry-url}")
    private String schemaRegistryUrl;

    private KafkaProducer<String, Payment> producer;

    @Bean(destroyMethod = "close")
    public KafkaProducer<String, Payment> getProducer() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put("schema.registry.url", schemaRegistryUrl);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producer = new KafkaProducer<>(configProps);
        return producer;
    }


    public void sendMessage(String topic, String messageKey, Payment message) throws InterruptedException, ExecutionException {
        try {
            ProducerRecord<String, Payment> record = new ProducerRecord<>(topic, messageKey, message);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Error sending message to topic " + topic, exception);
                } else {
                    logger.info("Message sent successfully to topic " + topic + ", partition: " + metadata.partition() + ": " + message.toString());
                }
            });
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
