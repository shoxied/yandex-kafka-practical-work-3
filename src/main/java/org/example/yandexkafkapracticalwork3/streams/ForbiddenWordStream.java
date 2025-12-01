package org.example.yandexkafkapracticalwork3.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.yandexkafkapracticalwork3.config.MyStreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Properties;

@Component
@Slf4j
public class ForbiddenWordStream {

    @Autowired
    public void buildPipelines(StreamsBuilder builder) throws InterruptedException {

        GlobalKTable<String, String> forbiddenWords = builder.globalTable(
                "forbidden_words",
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("forbidden-words-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
        );
        log.info("GlobalKTable: {} created", forbiddenWords);

        KStream<String, String> messageStream = builder.stream("messages");
        log.info("KStream: {} created", messageStream);

        KStream<String, String> filteredMessages = messageStream
                .flatMapValues(message -> {
                    log.info("Splitting message: {}", message);
                    return Arrays.asList(message.substring(1, message.length() - 1).split("\\s+"));
                })
                .leftJoin(
                        forbiddenWords,
                        (key, word) -> word.toLowerCase(),
                        (word, forbiddenWord) -> {
                            log.info("Filtering word: {}", word);
                            log.info("Forbidden word: {}", forbiddenWord);
                            if (forbiddenWord != null) {
                                log.info("Word '{}' is FORBIDDEN! Replacing with stars", word);
                                return "*".repeat(word.length());
                            } else {
                                log.info("Word '{}' is NOT FORBIDDEN!", word);
                                return word;
                            }
                        }
                )
                .groupByKey()
                .aggregate(
                        () -> "",
                        (key, word, aggregate) -> aggregate.isEmpty() ? word : aggregate + " " + word
                )
                .toStream();

        filteredMessages.to("filtered_messages", Produced.with(Serdes.String(), Serdes.String()));
    }
}
