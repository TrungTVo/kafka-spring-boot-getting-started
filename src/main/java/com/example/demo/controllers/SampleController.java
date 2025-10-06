package com.example.demo.controllers;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.consumer.KafkaListenerInspector;
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

    @PostMapping("produce")
    public String produce(@RequestBody MessageRequest messageRequest) {
        producer.sendMessage(messageRequest.getTopic(), messageRequest.getMessageKey(), messageRequest.getMessage());
        return "Message sent to Kafka topic: " + messageRequest.getTopic();
    }

    @GetMapping("consumer/list")
    public List<ConsumerModel> getConsumers() {
        return kafkaListenerInspector.getConsumers();
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
