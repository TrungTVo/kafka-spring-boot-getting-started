package com.example.demo.controllers;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.consumer.ConsumerGroupInspector;
import com.example.demo.consumer.KafkaListenerInspector;
import com.example.demo.models.ConsumerListenerModel;
import com.example.demo.models.ConsumerModel;
import com.example.demo.models.MessageRequest;
import com.example.demo.producer.Producer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;



@RestController
@RequestMapping("kafka")
public class SampleController {
    @Autowired
    private Producer producer;

    @Autowired
    private KafkaListenerInspector kafkaListenerInspector;

    @Autowired
    private ConsumerGroupInspector consumerGroupInspector;

    @PostMapping("produce")
    public String produce(@RequestBody MessageRequest messageRequest) {
        producer.sendMessage(messageRequest.getTopic(), messageRequest.getMessageKey(), messageRequest.getMessage());
        return "Message sent to Kafka topic: " + messageRequest.getTopic();
    }

    @GetMapping("consumer/listeners")
    public List<ConsumerListenerModel> getConsumerListeners() {
        return kafkaListenerInspector.getConsumerListeners();
    }

    @GetMapping("consumer/{groupId}/members")
    public List<ConsumerModel> getConsumersByGroupId(@PathVariable String groupId) throws Exception {
        return consumerGroupInspector.getConsumersByGroupId(groupId);
    }
    
    @GetMapping("consumer/{consumerListenerId}/start")
    public void consumerStart(@PathVariable String consumerListenerId) {
        kafkaListenerInspector.startListener(consumerListenerId);
    }
    
    @GetMapping("consumer/{consumerListenerId}/stop")
    public void consumerStop(@PathVariable String consumerListenerId) {
        kafkaListenerInspector.stopListener(consumerListenerId);
    }

    @GetMapping("consumer/{consumerListenerId}/pause")
    public void consumerPause(@PathVariable String consumerListenerId) {
        kafkaListenerInspector.pauseListener(consumerListenerId);
    }

    @GetMapping("consumer/{consumerListenerId}/resume")
    public void consumerResume(@PathVariable String consumerListenerId) {
        kafkaListenerInspector.resumeListener(consumerListenerId);
    }
}
