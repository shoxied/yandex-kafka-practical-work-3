package org.example.yandexkafkapracticalwork3.controller;

import lombok.RequiredArgsConstructor;
import org.example.yandexkafkapracticalwork3.dto.Message;
import org.example.yandexkafkapracticalwork3.producer.Producer;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("api/v1")
public class KafkaController {

    private final Producer producer;

    @GetMapping("produceForbiddenWord")
    public void produceForbiddenWord(@RequestParam String word) {
        producer.sendBlockedWord(word);
    }

    @PostMapping("sendMessage")
    public void sendMessage(@RequestBody Message message) {
        producer.sendMessage(message);
    }

    @GetMapping("blockUser")
    public void blockUser(@RequestParam String user,
                          @RequestParam String blockedUser) throws InterruptedException {
        producer.blockUser(user, blockedUser);
    }

}
