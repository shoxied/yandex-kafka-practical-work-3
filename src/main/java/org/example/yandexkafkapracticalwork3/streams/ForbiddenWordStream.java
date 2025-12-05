package org.example.yandexkafkapracticalwork3.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.*;
import org.example.yandexkafkapracticalwork3.dto.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@Slf4j
public class ForbiddenWordStream {

    @Autowired
    public void buildPipelines(StreamsBuilder builder) {

        JsonSerde<Message> messageSerde = new JsonSerde<>(Message.class);
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put(JsonDeserializer.TRUSTED_PACKAGES, "org.example.yandexkafkapracticalwork3.dto");
        messageSerde.configure(serdeProps, false);

        GlobalKTable<String, String> blockedUsers = builder.globalTable(
                "blocked_users",
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("blocked-users-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
        );

        GlobalKTable<String, String> forbiddenWords = builder.globalTable(
                "forbidden_words",
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("forbidden-words-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
        );

        // Поток исходных сообщений
        KStream<String, Message> rawMessages = builder.stream(
                "messages",
                Consumed.with(Serdes.String(), messageSerde)
        );

        KStream<String, Message> filtered = rawMessages.leftJoin(
                blockedUsers,
                (key, message) -> message.getReceiver(),
                (message, blockedList) -> {
                    if (blockedList == null) return message;
                    List<String> blockedSenders = Arrays.asList(blockedList.split(","));
                    return blockedSenders.contains(message.getSender()) ? null : message;
                }
        ).filter((k, v) -> v != null);

        KStream<String, Message> cleanedMessages = filtered.transformValues(
                () -> new ValueTransformerWithKey<String, Message, Message>() {

                    private KeyValueStore<String, String> forbiddenStore;

                    @Override
                    public void init(ProcessorContext context) {
                        forbiddenStore = (KeyValueStore<String, String>) context.getStateStore("forbidden-words-store");
                    }

                    @Override
                    public Message transform(String readOnlyKey, Message value) {
                        if (value == null) return null;

                        String text = value.getMessage();
                        String[] words = text.split("\\s+");

                        for (int i = 0; i < words.length; i++) {
                            if (forbiddenStore.get(words[i]) != null) {
                                words[i] = "*".repeat(words[i].length()); // заменяем запрещенное слово
                            }
                        }

                        value.setMessage(String.join(" ", words));
                        return value;
                    }

                    @Override
                    public void close() {
                    }
                }
        );

        filtered.to("filtered_messages");
    }
}
