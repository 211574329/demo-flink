package com.echo.ranking.task;


import com.echo.common.bo.OrderFlinkBO;
import com.echo.ranking.config.MqConfig;
import com.echo.ranking.constant.RedisConstant;
import com.echo.ranking.function.ProductRankingSlink;
import com.echo.ranking.mapper.RedisProductRankingMapper;
import com.echo.ranking.poly.ProductRanking;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

/**
 * 滚动窗口
 * 商品总排行榜
 */
public class RollRankingFlink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 数据源
        DataStream<OrderFlinkBO> dataStream = env.addSource(MqConfig.getRMQSource()).name("RabbitMQ_Source");

        DataStream<ProductRanking> stream = dataStream.keyBy(OrderFlinkBO::getSkuId)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .sum("allCount")
                .map((MapFunction<OrderFlinkBO, ProductRanking>) bo -> ProductRanking.builder().skuId(bo.getSkuId()).skuName(bo.getSkuName()).count(bo.getAllCount()).build());

/*        DataStream<ProductRanking> stream2 = dataStream.map((MapFunction<OrderFlinkBO, Tuple2<Long, Integer>>) order -> Tuple2.of(order.getSkuId(), order.getAllCount()))
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .sum(1)
                .map((MapFunction<Tuple2<Long, Integer>, ProductRanking>) value -> ProductRanking.builder().skuId(value.f0).count(value.f1).build());*/

                /*.map((MapFunction<Tuple2<Long, Integer>, ProductRanking>) value -> {
                    ProductRanking ranking = new ProductRanking();
                    ranking.setSkuId(value.f0);
                    ranking.setCount(value.f1);
                    return ProductRanking.builder().skuId(value.f0).count(value.f1).build();
                });*/

        FlinkJedisPoolConfig poolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost(RedisConstant.HOST)
                .setPort(RedisConstant.PORT)
                .setPassword(RedisConstant.PASSWORD)
                .setDatabase(RedisConstant.DATABASE)
                .setTimeout(RedisConstant.TIMEOUT).build();
        // 添加到redis
        stream.addSink(new RedisSink<>(poolConfig, new RedisProductRankingMapper()));

        env.execute("Roll_Ranking");
    }

}
