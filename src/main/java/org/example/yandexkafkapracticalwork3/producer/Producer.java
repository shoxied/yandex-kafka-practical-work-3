package org.example.yandexkafkapracticalwork3.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.example.yandexkafkapracticalwork3.dto.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
@Component
public class Producer {

    private final StreamsBuilderFactoryBean factoryBean;

    private final KafkaTemplate<String, Message> kafkaTemplateJson;
    private final KafkaTemplate<String, String> kafkaTemplateString;

    @Autowired
    public Producer(StreamsBuilderFactoryBean factoryBean,
                    @Qualifier("kafkaTemplateJson")
                    KafkaTemplate<String, Message> kafkaTemplateJson,
                    @Qualifier("kafkaTemplateString")
                    KafkaTemplate<String, String> kafkaTemplateString) {

        this.factoryBean = factoryBean;
        this.kafkaTemplateJson = kafkaTemplateJson;
        this.kafkaTemplateString = kafkaTemplateString;
    }

    public void blockUser(String user, String blockedUser) throws InterruptedException, ExecutionException {

        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();


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
            kafkaTemplateString.send("blocked_users", user, blockedUser);
        }
        else {
            blockedList = blockedList + "," + blockedUser;
            kafkaTemplateString.send("blocked_users", user, blockedList).get();
        }
    }

    public void sendMessage(Message message) {
        kafkaTemplateJson.send("messages", String.valueOf(UUID.randomUUID()), message);
    }

    public void sendBlockedWord(String word) {
        kafkaTemplateString.send("forbidden_words", word, word);
    }
}
