package org.example.yandexkafkapracticalwork3;

import org.example.yandexkafkapracticalwork3.utils.KafkaSetUpper;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class YandexKafkaPracticalWork3Application {

    public static void main(String[] args) {

        new SpringApplicationBuilder(YandexKafkaPracticalWork3Application.class)
                .initializers(new KafkaSetUpper())
                .run(args);
    }

}
