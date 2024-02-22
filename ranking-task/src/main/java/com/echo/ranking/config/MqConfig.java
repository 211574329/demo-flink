package com.echo.ranking.config;

import com.echo.common.bo.OrderFlinkBO;
import com.echo.common.enums.RabbitEnum;
import com.echo.ranking.constant.RabbitConstant;
import com.echo.ranking.deser.OrderDeserializer;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * mq配置类
 */
public class MqConfig {

    /**
     * 获取rabbitmq配置
     *
     * @return rabbitmq配置
     */
    public static RMQConnectionConfig getRMQConfig() {
        // 创建RabbitMQ连接配置
        return new RMQConnectionConfig.Builder()
                .setHost(RabbitConstant.HOST)
                .setPort(RabbitConstant.PORT)
                .setUserName(RabbitConstant.USERNAME)
                .setPassword(RabbitConstant.PASSWORD)
                .setVirtualHost(RabbitConstant.VIRTUALHOST)
                .build();
    }

    /**
     * 获取rabbitmq数据源
     *
     * @return rabbitmq数据源
     */
    public static RMQSource<OrderFlinkBO> getRMQSource() {
        // RabbitMq 数据源配置
        return new RMQSource<>(
                // 配置
                getRMQConfig(),
                // 队列名称
                RabbitEnum.ORDER_QUEUE.getName(),
                // 序列化模式
                new OrderDeserializer()
        );
    }

}
