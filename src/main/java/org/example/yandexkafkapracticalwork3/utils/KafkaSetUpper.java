package org.example.yandexkafkapracticalwork3.utils;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

import java.util.concurrent.ExecutionException;

public class KafkaSetUpper implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext context) {

        Environment environment = context.getEnvironment();

        String bootstrapServers = environment.getProperty("spring.kafka.bootstrap-servers");

        Kafka kafka = new Kafka(bootstrapServers);
        kafka.setUp();

    }
}
