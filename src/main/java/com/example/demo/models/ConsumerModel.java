package com.example.demo.models;

import java.util.List;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class ConsumerModel {
    private String clientId;
    private String groupId;
    private String consumerMemberId;
    private List<PartitionInfoDTO> assignedPartitions;

    public ConsumerModel(String clientId, String groupId, String consumerMemberId, List<PartitionInfoDTO> assignedPartitions) {
        this.clientId = clientId;
        this.groupId = groupId;
        this.consumerMemberId = consumerMemberId;
        this.assignedPartitions = assignedPartitions;
    }
}
