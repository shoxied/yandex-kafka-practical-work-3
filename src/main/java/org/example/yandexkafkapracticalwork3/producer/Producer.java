package org.example.yandexkafkapracticalwork3.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.example.yandexkafkapracticalwork3.dto.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
public class Producer {

    private final KafkaStreams kafkaStreams;

    private final KafkaTemplate<String, Message> kafkaTemplateJson;
    private final KafkaTemplate<String, String> kafkaTemplateString;

    @Autowired
    public Producer(KafkaStreams kafkaStreams, @Qualifier("kafkaTemplateJson")
                           KafkaTemplate<String, Message> kafkaTemplateJson,
                    @Qualifier("kafkaTemplateString")
                           KafkaTemplate<String, String> kafkaTemplateString) {
        this.kafkaStreams = kafkaStreams;
        this.kafkaTemplateJson = kafkaTemplateJson;
        this.kafkaTemplateString = kafkaTemplateString;
    }

    public void sendMessage(Message message) {
        kafkaTemplateJson.send("messages", String.valueOf(UUID.randomUUID()), message);
    }

    public void sendBlockedWord(String word) {




        kafkaTemplateString.send("forbidden_words", word, word);
    }

    public void blockUser(String user, String blockedUser) throws InterruptedException {


        while (kafkaStreams.state() != KafkaStreams.State.RUNNING){
            Thread.sleep(500);

            if (kafkaStreams.state() == KafkaStreams.State.PENDING_SHUTDOWN) return;
        }

        ReadOnlyKeyValueStore<String, String> store =
                kafkaStreams.store(
                        StoreQueryParameters.fromNameAndType(
                                "blocked-users-store",
                                QueryableStoreTypes.keyValueStore()
                        )
                );

        String blockedList = store.get(user);
        if (blockedList == null || blockedList.isEmpty()) {
            blockedList = blockedUser;
        }
        else {
            blockedList = blockedList + "," + blockedUser;
        }

        kafkaTemplateString.send("blocked_users", user, blockedList);
    }
}
