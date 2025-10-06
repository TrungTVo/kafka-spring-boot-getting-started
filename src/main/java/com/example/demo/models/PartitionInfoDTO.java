package com.example.demo.models;

import org.apache.kafka.common.TopicPartition;

public record PartitionInfoDTO(String topic, int partition) {
    public static PartitionInfoDTO from(TopicPartition tp) {
        return new PartitionInfoDTO(tp.topic(), tp.partition());
    }
}

