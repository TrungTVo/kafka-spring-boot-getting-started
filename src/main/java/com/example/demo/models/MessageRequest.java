package com.example.demo.models;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class MessageRequest {
    private String topic;
    private String messageKey;
    private Object message;

    public MessageRequest(String topic, String messageKey, Object message) {
        this.topic = topic;
        this.messageKey = messageKey;
        this.message = message;
    }
}
