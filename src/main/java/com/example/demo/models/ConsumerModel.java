package com.example.demo.models;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class ConsumerModel {
    private String listenerId;
    private String groupId;
    private boolean isRunning;
    private Map<String, List<PartitionInfoDTO>> topicPartitions;

    public ConsumerModel(String listenerId, String groupId, boolean isRunning, Map<String, List<PartitionInfoDTO>> topicPartitions) {
        this.listenerId = listenerId;
        this.groupId = groupId;
        this.isRunning = isRunning;
        this.topicPartitions = topicPartitions;
    }
}
