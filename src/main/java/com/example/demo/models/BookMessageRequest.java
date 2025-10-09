package com.example.demo.models;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class BookMessageRequest {
    private String topic;
    private String messageKey;
    private Book message;

    public BookMessageRequest(String topic, String messageKey, Book message) {
        this.topic = topic;
        this.messageKey = messageKey;
        this.message = message;
    }
}
