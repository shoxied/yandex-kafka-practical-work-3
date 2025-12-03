package org.example.yandexkafkapracticalwork3.service;

public interface KafkaService {
    void sendMessage(String message);
    void sendBlockedWord(String blockedWord);
}
