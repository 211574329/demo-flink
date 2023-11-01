package com.echo.function;

import com.echo.bo.OrderFlinkBO;
import com.echo.poly.OrderRanking;
import org.apache.flink.api.common.functions.AggregateFunction;

public class OrderAggregate implements AggregateFunction<OrderFlinkBO, OrderRanking, OrderRanking> {
    @Override
    public OrderRanking createAccumulator() {
        return new OrderRanking();
    }

    @Override
    public OrderRanking add(OrderFlinkBO order, OrderRanking ranking) {
        ranking.setAreaNo(order.getAreaNo());
        ranking.setAreaName(order.getAreaName());
        ranking.setSkuId(order.getSkuId());
        ranking.setSkuName(order.getSkuName());
        ranking.setCount(ranking.getCount() + order.getAllCount());
        return ranking;
    }

    @Override
    public OrderRanking getResult(OrderRanking ranking) {
        return ranking;
    }

    @Override
    public OrderRanking merge(OrderRanking a, OrderRanking b) {
        return a;
    }
}
