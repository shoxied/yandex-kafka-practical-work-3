package org.example.yandexkafkapracticalwork3.producer;

import lombok.extern.slf4j.Slf4j;
import org.example.yandexkafkapracticalwork3.dto.Message;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
public class Producer {

    private final KafkaTemplate<String, Message> kafkaTemplateJson;
    private final KafkaTemplate<String, String> kafkaTemplateString;

    public Producer(@Qualifier("kafkaTemplateJson")
                           KafkaTemplate<String, Message> kafkaTemplateJson,
                    @Qualifier("kafkaTemplateString")
                           KafkaTemplate<String, String> kafkaTemplateString) {
        this.kafkaTemplateJson = kafkaTemplateJson;
        this.kafkaTemplateString = kafkaTemplateString;
    }

    public void sendMessage(Message message) {
        kafkaTemplateJson.send("messages", String.valueOf(UUID.randomUUID()), message);
    }

    public void sendBlockedWord(String word) {
        kafkaTemplateString.send("forbiddenWords", String.valueOf(UUID.randomUUID()), word);
    }

    public void blockUser(String user, String blockedUser) {
        kafkaTemplateString.send("blocked_users", user, blockedUser);
    }
}
