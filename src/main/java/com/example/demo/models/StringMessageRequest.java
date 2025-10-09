package com.example.demo.models;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class StringMessageRequest {
    private String topic;
    private String messageKey;
    private String message;

    public StringMessageRequest(String topic, String messageKey, String message) {
        this.topic = topic;
        this.messageKey = messageKey;
        this.message = message;
    }
}
