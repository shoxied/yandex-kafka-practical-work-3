package org.example.yandexkafkapracticalwork3.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ForbiddenWordStream {

    @Autowired
    public void buildPipelines(StreamsBuilder builder) throws Exception {
        
    }
}
