package com.echo.common.enums;

import lombok.Getter;

/**
 * rabbit枚举
 */
@Getter
public enum RabbitEnum {

    ORDER_QUEUE("order_inform_flink_queue");

    /**
     * name
     */
    private final String name;

    RabbitEnum(String name) {
        this.name = name;
    }
}
