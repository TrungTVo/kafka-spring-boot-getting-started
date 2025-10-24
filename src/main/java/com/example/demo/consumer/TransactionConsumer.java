package com.example.demo.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import com.example.demo.Payment;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

@Service
public class TransactionConsumer {
    private final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String BOOTSTRAP_SERVERS;

    @Value("${spring.kafka.properties.schema-registry-url}")
    private String SCHEMA_REGISTRY_URL;

    private static final String TOPIC = "transactions";

    private KafkaConsumer<String, Payment> consumer;

    @Bean(destroyMethod = "close")
    public KafkaConsumer<String, Payment> kafkaConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(props);
        subscribeTopics();
        return consumer;
    }

    public void subscribeTopics() {
        if (consumer != null) {
            consumer.subscribe(Collections.singletonList(TOPIC));
        }
    }

    public void consumeRecords() {
        try {
            ConsumerRecords<String, Payment> records = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, Payment> record : records) {
                this.logger.info(String.format("Consumed record from topic %s-%d, offset %d: %s",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value().toString()
                ));
            }
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
        } finally {
            consumer.close();
        }
    }
}
