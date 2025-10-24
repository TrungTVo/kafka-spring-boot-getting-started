package com.example.demo.models;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import com.example.demo.Payment;

@NoArgsConstructor
@Getter
@Setter
public class TransactionMessageRequest {
    private String topic;
    private String messageKey;
    private Payment message;

    public TransactionMessageRequest(String topic, String messageKey, Payment message) {
        this.topic = topic;
        this.messageKey = messageKey;
        this.message = message;
    }
}
