package org.example.yandexkafkapracticalwork3.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BlockedUser {

    private Integer blockingUserId;
    private Integer blockedUserId;

}
