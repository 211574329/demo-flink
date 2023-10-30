package com.echo.function;

import com.echo.bo.OrderFlinkBO;
import com.echo.poly.OrderSummary;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class OrderSumApply implements AllWindowFunction<OrderFlinkBO, OrderSummary, GlobalWindow> {

   /* @Override
    public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<OrderFlinkBO> iterable, Collector<OrderSummary> collector) throws Exception {
        Long skuId = tuple.getField(0);
        String skuName = null;  // 根据skuId查询sku名称，省略实现过程
        Integer count = 0;
        Iterator<OrderFlinkBO> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            OrderFlinkBO order = iterator.next();
            count += order.getAllCount();
            skuName = order.getSkuName();
        }
        collector.collect(new OrderSummary(skuId, skuName, count));
    }*/

    @Override
    public void apply(GlobalWindow globalWindow, Iterable<OrderFlinkBO> orders, Collector<OrderSummary> out) throws Exception {
        // 汇总订单数量
        Map<Long, OrderSummary> map = new HashMap<>();
        OrderSummary summary = new OrderSummary();
        for (OrderFlinkBO order : orders) {
            Long skuId = order.getSkuId();
            Integer allCount = order.getAllCount();
            OrderSummary orderSummary = map.get(skuId);
            if (orderSummary != null) {
                summary.setCount(orderSummary.getCount() + allCount);
            } else {
                summary.setCount(allCount);
            }
            summary.setSkuId(skuId);
            summary.setSkuName(order.getSkuName());
            map.put(skuId, summary);
            System.out.println("订单统计" + summary);
        }
        // 放入out
        map.forEach((k, v) -> out.collect(v));
    }
}
