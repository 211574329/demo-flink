package com.echo.ranking.deser;

import com.alibaba.fastjson2.JSON;
import com.echo.common.bo.OrderFlinkBO;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * 消息序列化
 */
@Slf4j
public  class OrderDeserializer implements DeserializationSchema<OrderFlinkBO> {

    public OrderFlinkBO deserialize(byte[] bytes) throws IOException {
        log.info("consumer order message:[{}]", new String(bytes));
        String json = new String(bytes, "UTF-8");
        return JSON.parseObject(json, OrderFlinkBO.class);
    }

    public boolean isEndOfStream(OrderFlinkBO orderFlinkBO) {
        return false;
    }

    public TypeInformation<OrderFlinkBO> getProducedType() {
        return TypeInformation.of(OrderFlinkBO.class);
    }

}
