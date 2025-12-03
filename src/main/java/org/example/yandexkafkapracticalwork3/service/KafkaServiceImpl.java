package org.example.yandexkafkapracticalwork3.service;

import lombok.RequiredArgsConstructor;
import org.example.yandexkafkapracticalwork3.producer.Producer;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaServiceImpl implements KafkaService {

    private final Producer messageProducer;

    @Override
    public void sendMessage(String message) {



        messageProducer.sendMessage("messages", );
    }

    @Override
    public void sendBlockedWord(String blockedWord) {

    }
}
