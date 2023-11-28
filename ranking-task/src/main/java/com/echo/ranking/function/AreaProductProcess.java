package com.echo.ranking.function;

import com.echo.common.bo.OrderFlinkBO;
import com.echo.ranking.poly.AreaProductRanking;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class AreaProductProcess  extends ProcessWindowFunction<OrderFlinkBO, AreaProductRanking, Tuple2<Integer, Long>, GlobalWindow> {

    @Override
    public void process(Tuple2<Integer, Long> tuple, Context context, Iterable<OrderFlinkBO> elements, Collector<AreaProductRanking> out) throws Exception {
        Integer areaId = tuple.f0;
        Long skuId = tuple.f1;
        String skuName = null;
        Integer count = 0;
        for (OrderFlinkBO order : elements) {
            skuName = order.getSkuName();
            count += order.getAllCount();
        }
        AreaProductRanking ranking = new AreaProductRanking();
        ranking.setAreaId(areaId);
        ranking.setAreaName(elements.iterator().next().getAreaName());
        ranking.setSkuId(skuId);
        ranking.setSkuName(skuName);
        ranking.setCount(count);
        System.out.println(ranking);
        out.collect(ranking);
    }
}
