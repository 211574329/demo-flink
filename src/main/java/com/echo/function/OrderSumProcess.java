package com.echo.function;

import com.echo.bo.OrderFlinkBO;
import com.echo.poly.OrderSummary;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class OrderSumProcess extends ProcessAllWindowFunction<OrderFlinkBO, OrderSummary, GlobalWindow> {

    @Override
    public void process(ProcessAllWindowFunction<OrderFlinkBO, OrderSummary, GlobalWindow>.Context context, Iterable<OrderFlinkBO> orders, Collector<OrderSummary> out) throws Exception {
        // 汇总订单数量
        Map<Long, OrderSummary> map = new HashMap<>();
        for (OrderFlinkBO order : orders) {
            Long skuId = order.getSkuId();
            Integer allCount = order.getAllCount();
            OrderSummary orderSummary = map.get(skuId);
            if (orderSummary != null) {
                orderSummary.setCount(orderSummary.getCount() + allCount);
            } else {
                orderSummary = new OrderSummary();
                orderSummary.setSkuId(skuId);
                orderSummary.setSkuName(order.getSkuName());
                orderSummary.setCount(allCount);
            }
            map.put(skuId, orderSummary);
        }
        // 放入out
        map.forEach((k, v) -> out.collect(v));
    }

}
