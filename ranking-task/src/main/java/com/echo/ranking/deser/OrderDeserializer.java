package com.echo.ranking.deser;

import com.alibaba.fastjson2.JSON;
import com.echo.common.bo.OrderFlinkBO;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 消息序列化
 */
@Slf4j
public  class OrderDeserializer implements RMQDeserializationSchema<OrderFlinkBO> {


    @Override
    public void deserialize(Envelope envelope, AMQP.BasicProperties basicProperties, byte[] bytes, RMQCollector<OrderFlinkBO> collector) throws IOException {
        log.info("consumer order message:[{}]", new String(bytes));
        String json = new String(bytes, StandardCharsets.UTF_8);
        collector.collect(JSON.parseObject(json, OrderFlinkBO.class));
    }

    public boolean isEndOfStream(OrderFlinkBO orderFlinkBO) {
        return false;
    }

    public TypeInformation<OrderFlinkBO> getProducedType() {
        return TypeInformation.of(OrderFlinkBO.class);
    }

}
