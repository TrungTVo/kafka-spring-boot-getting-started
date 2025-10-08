package com.example.demo.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import com.example.demo.models.ConsumerListenerModel;
import com.example.demo.models.PartitionInfoDTO;

@Component
public class KafkaListenerInspector {
    private final Logger logger = LoggerFactory.getLogger(KafkaListenerInspector.class);

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    public List<ConsumerListenerModel> getConsumerListeners() {
        List<ConsumerListenerModel> consumers = new ArrayList<>();
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            logger.info("Consumer Listener ID: " + container.getListenerId() + ", groupId: " + container.getGroupId() + ", isRunning: " + container.isRunning());
            Map<String, Collection<TopicPartition>> assignments = container.getAssignmentsByClientId();

            Map<String, List<PartitionInfoDTO>> topicPartitions = new HashMap<>();
            if (assignments != null) {
                topicPartitions = assignments.entrySet().stream()
                    .collect(Collectors.toMap(
                        entry -> entry.getKey(),
                        entry -> entry.getValue().stream()
                            .map(tp -> PartitionInfoDTO.from(tp))
                            .collect(Collectors.toList())
                    ));
            }
            consumers.add(new ConsumerListenerModel(
                container.getListenerId(), 
                container.getGroupId(), 
                container.isRunning(),
                topicPartitions
            ));
        }
        return consumers;
    }

    // Start the listener
    public void startListener(String consumerListenerId) {
        MessageListenerContainer listenerContainer = registry.getListenerContainer(consumerListenerId);
        if (listenerContainer != null && !listenerContainer.isRunning()) {
            listenerContainer.start();
            logger.info("Started listener: " + consumerListenerId);
        }
    }

    // Stop the listener
    public void stopListener(String consumerListenerId) {
        MessageListenerContainer listenerContainer = registry.getListenerContainer(consumerListenerId);
        if (listenerContainer != null && listenerContainer.isRunning()) {
            listenerContainer.stop();
            logger.info("Stopped listener: " + consumerListenerId);
        }
    }

    // Pause the listener
    public void pauseListener(String consumerListenerId) {
        MessageListenerContainer listenerContainer = registry.getListenerContainer(consumerListenerId);
        if (listenerContainer != null && listenerContainer.isRunning()) {
            listenerContainer.pause();
            logger.info("Paused listener: " + consumerListenerId);
        }
    }

    // Resume the listener
    public void resumeListener(String consumerListenerId) {
        MessageListenerContainer listenerContainer = registry.getListenerContainer(consumerListenerId);
        if (listenerContainer != null && listenerContainer.isPauseRequested()) {
            listenerContainer.resume();
            logger.info("Resumed listener: " + consumerListenerId);
        }
    }
}
