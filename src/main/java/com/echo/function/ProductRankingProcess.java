package com.echo.function;

import com.echo.bo.OrderFlinkBO;
import com.echo.poly.ProductRanking;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class ProductRankingProcess extends ProcessAllWindowFunction<OrderFlinkBO, ProductRanking, GlobalWindow> {

    @Override
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
    }

}
