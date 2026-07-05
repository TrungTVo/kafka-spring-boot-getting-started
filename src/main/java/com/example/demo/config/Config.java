package com.example.demo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;


@Configuration
public class Config {
    private Logger logger = LoggerFactory.getLogger(Config.class);

    private final KafkaAdmin kafkaAdmin;

    public Config(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    @Bean
    public ApplicationRunner runner() {
        return args -> {
            this.logger.info("Application Runner started...");
            this.logger.info("Kafka Cluster ID: " + kafkaAdmin.clusterId());
        };
    }

    @Bean
    public NewTopic books() {
        return TopicBuilder.name("books")
                .partitions(2)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic hello() {
        return TopicBuilder.name("hello")
                .partitions(2)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic transactions() {
        return TopicBuilder.name("transactions")
                .partitions(2)
                .replicas(1)
                .compact()
                .build();
    }
}
