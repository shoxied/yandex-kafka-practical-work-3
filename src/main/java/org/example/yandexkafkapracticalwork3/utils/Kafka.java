package org.example.yandexkafkapracticalwork3.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
public class Kafka {

    private final String bootstrapAddress;

    public Kafka(String bootstrapAddress) {
        this.bootstrapAddress = bootstrapAddress;
    }

    public void setUp(){

        try (AdminClient admin = AdminClient.create(Collections.singletonMap("bootstrap.servers", bootstrapAddress));) {

            ListTopicsOptions options = new ListTopicsOptions();
            ListTopicsResult topics = admin.listTopics(options);
            Set<String> currentTopicList = topics.names().get();

            if (currentTopicList.isEmpty()) {
                createTopics(admin);
                loadData();
            }
            else log.info("Topics already exist");

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void createTopics(AdminClient admin) throws ExecutionException, InterruptedException {

        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "compact");

        NewTopic messages = new NewTopic("messages", 1, (short) 1).configs(configs);
        NewTopic filteredMessages = new NewTopic("filtered_messages", 1, (short) 1).configs(configs);
        NewTopic blockedUsers = new NewTopic("blocked_users", 1, (short) 1).configs(configs);
        NewTopic forbiddenWords = new NewTopic("forbidden_words", 1, (short) 1).configs(configs);

        admin.createTopics(Arrays.asList(messages, filteredMessages, blockedUsers, forbiddenWords)).all().get();

        log.info("Topics created");

    }

    private void loadData(){

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            String[] messages = {
                    "kafka",
                    "streams",
                    "слова"
            };

            for (int i = 0; i < messages.length; i++) {
                String value = messages[i];

                ProducerRecord<String, String> record = new ProducerRecord<>("forbidden_words", value, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Successfully sent message {}", value);
                    } else {
                        log.error("Failed to send message {}", value);
                    }
                });
            }

            producer.flush();
        }
    }
}
