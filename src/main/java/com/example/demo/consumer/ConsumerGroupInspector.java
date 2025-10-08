package com.example.demo.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import com.example.demo.models.ConsumerModel;
import com.example.demo.models.PartitionInfoDTO;

@Component
public class ConsumerGroupInspector {
    @Autowired
    private KafkaAdmin kafkaAdmin;

    public List<ConsumerModel> getConsumersByGroupId(String groupId) throws Exception {
        List<ConsumerModel> consumers = new ArrayList<>();
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            DescribeConsumerGroupsResult result = adminClient.describeConsumerGroups(Collections.singletonList(groupId));
            ConsumerGroupDescription group_description = result.all().get().get(groupId);

            group_description.members().forEach((MemberDescription member) -> {
                System.out.println("Client ID: " + member.clientId() + ", Consumer ID: " + member.consumerId() + ", Host: " + member.host() + ", Partitions: " + member.assignment());
                List<PartitionInfoDTO> assignedPartitions = member.assignment().topicPartitions()
                                                                    .stream()
                                                                    .map((TopicPartition tp) -> PartitionInfoDTO.from(tp))
                                                                    .collect(Collectors.toList());
                ConsumerModel consumer = new ConsumerModel(
                                                member.clientId(), 
                                                groupId, 
                                                member.consumerId(), 
                                                assignedPartitions);
                consumers.add(consumer);
            });
        }
        return consumers;
    }
}
