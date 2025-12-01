package org.example.yandexkafkapracticalwork3.controller;


import lombok.RequiredArgsConstructor;
import org.example.yandexkafkapracticalwork3.producer.MessageProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("api/v1")
public class KafkaController {

    private final MessageProducer messageProducer;

    @GetMapping("produceForbiddenWord")
    public void produceForbiddenWord(@RequestParam String word) {
        messageProducer.send("forbidden_words", word);
    }

    @GetMapping("sendMessage")
    public void sendMessage(@RequestParam String message) {
        messageProducer.send("messages", message);
    }

}
