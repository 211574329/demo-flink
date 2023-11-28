package com.echo.ranking.function;

import com.echo.common.bo.OrderFlinkBO;
import com.echo.ranking.poly.AreaProductRanking;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AreaProductAggregate implements AggregateFunction<OrderFlinkBO, AreaProductRanking, AreaProductRanking> {
    @Override
    public AreaProductRanking createAccumulator() {
        return new AreaProductRanking();
    }

    @Override
    public AreaProductRanking add(OrderFlinkBO order, AreaProductRanking ranking) {
        System.out.println("order-" + order);
        System.out.println("rank-" + ranking);
        ranking.setAreaId(order.getAreaId());
        ranking.setAreaName(order.getAreaName());
        ranking.setSkuId(order.getSkuId());
        ranking.setSkuName(order.getSkuName());
        if (ranking.getCount() == null) {
            ranking.setCount(0);
        }
        ranking.setCount(ranking.getCount() + order.getAllCount());
        return ranking;
    }

    @Override
    public AreaProductRanking getResult(AreaProductRanking ranking) {
        return ranking;
    }

    @Override
    public AreaProductRanking merge(AreaProductRanking a, AreaProductRanking b) {
        a.setCount(a.getCount() + b.getCount());
        return a;
    }
}
