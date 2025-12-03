package org.example.yandexkafkapracticalwork3.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Message {

    private Integer sender;
    private String message;
    private Integer receiver;

}
