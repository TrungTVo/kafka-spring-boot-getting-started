package com.example.demo.controllers;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.consumer.TransactionConsumer;
import com.example.demo.consumer.inspector.ConsumerGroupInspector;
import com.example.demo.consumer.inspector.KafkaListenerInspector;
import com.example.demo.models.BookMessageRequest;
import com.example.demo.models.ConsumerListenerModel;
import com.example.demo.models.ConsumerModel;
import com.example.demo.models.StringMessageRequest;
import com.example.demo.models.TransactionMessageRequest;
import com.example.demo.producer.BookProducer;
import com.example.demo.producer.StringProducer;
import com.example.demo.producer.TransactionProducer;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;



@RestController
@RequestMapping("kafka")
public class SampleController {
    @Autowired
    private StringProducer stringProducer;

    @Autowired
    private BookProducer bookProducer;

    @Autowired
    private TransactionProducer transactionProducer;

    @Autowired
    private KafkaListenerInspector kafkaListenerInspector;

    @Autowired
    private ConsumerGroupInspector consumerGroupInspector;

    @Autowired
    private TransactionConsumer transactionConsumer;

    @PostMapping("string/produce")
    public String produce(@RequestBody StringMessageRequest messageRequest) {
        stringProducer.sendMessage(messageRequest.getTopic(), messageRequest.getMessageKey(), messageRequest.getMessage());
        return "Message sent to Kafka topic: " + messageRequest.getTopic();
    }

    @PostMapping("book/produce")
    public String produceJson(@RequestBody BookMessageRequest messageRequest) {
        bookProducer.sendMessage(messageRequest.getTopic(), messageRequest.getMessageKey(), messageRequest.getMessage());
        return "Message sent to Kafka topic: " + messageRequest.getTopic();
    }

    @PostMapping("transaction/produce")
    public String produceTransaction(@RequestBody TransactionMessageRequest messageRequest) throws InterruptedException, ExecutionException {
        transactionProducer.sendMessage(messageRequest.getTopic(), messageRequest.getMessageKey(), messageRequest.getMessage());
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

    @GetMapping("consumer/transactions")
    public void consumeTransactions() {
        transactionConsumer.consumeRecords();
    }
}
