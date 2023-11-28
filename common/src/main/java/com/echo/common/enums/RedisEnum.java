package com.echo.common.enums;

import lombok.Getter;

/**
 * redis key
 */
@Getter
public enum RedisEnum {

    /**
     * 商品总排行
     */
    SUM_PRODUCT_RANK("Flink:Product:Ranking"),

    /**
     * 商品总排行名称
     */
    SUM_PRODUCT_RANK_NAMES("Flink:Product:Names");

    /**
     * key
     */
    private final String key;

    RedisEnum(String key) {
        this.key = key;
    }
}
