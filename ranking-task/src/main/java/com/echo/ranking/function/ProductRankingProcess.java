package com.echo.ranking.function;

import com.echo.common.bo.OrderFlinkBO;
import com.echo.ranking.poly.ProductRanking;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class ProductRankingProcess extends ProcessWindowFunction<OrderFlinkBO, ProductRanking, Long, GlobalWindow> {

    /*@Override
    public void process(ProcessAllWindowFunction<OrderFlinkBO, ProductRanking, GlobalWindow>.Context context, Iterable<OrderFlinkBO> orders, Collector<ProductRanking> out) throws Exception {
        // 汇总订单数量
        Map<Long, ProductRanking> map = new HashMap<>();
        for (OrderFlinkBO order : orders) {
            Long skuId = order.getSkuId();
            Integer allCount = order.getAllCount();
            ProductRanking productRanking = map.get(skuId);
            if (productRanking != null) {
                productRanking.setCount(productRanking.getCount() + allCount);
            } else {
                productRanking = new ProductRanking();
                productRanking.setSkuId(skuId);
                productRanking.setSkuName(order.getSkuName());
                productRanking.setCount(allCount);
            }
            map.put(skuId, productRanking);
        }
        // 放入out
        map.forEach((k, v) -> out.collect(v));
    }*/

    @Override
    public void process(Long skuId, ProcessWindowFunction<OrderFlinkBO, ProductRanking, Long, GlobalWindow>.Context context, Iterable<OrderFlinkBO> orders, Collector<ProductRanking> out) throws Exception {
        int count = 0;
        for (OrderFlinkBO order : orders) {
            count += order.getAllCount();
        }

        // 创建OrderSummary对象并保存结果
        ProductRanking ranking = new ProductRanking();
        ranking.setSkuId(skuId);
        ranking.setSkuName(orders.iterator().next().getSkuName());
        ranking.setCount(count);
        out.collect(ranking);
    }
}
