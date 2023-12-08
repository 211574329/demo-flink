package com.echo.ranking.task;


import com.echo.common.bo.OrderFlinkBO;
import com.echo.common.enums.RabbitEnum;
import com.echo.ranking.constant.RabbitConstant;
import com.echo.ranking.function.*;
import com.echo.ranking.deser.OrderDeserializer;
import com.echo.ranking.poly.ProductRanking;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * 商品总排行榜
 */
public class RankingFlink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启定时检查点
        //env.enableCheckpointing(5000);
        // 设置流处理时间特性为事件时间
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置并行度为 1
        //env.setParallelism(1);

        // 创建RabbitMQ连接配置
        RMQConnectionConfig config = new RMQConnectionConfig.Builder()
                .setHost(RabbitConstant.HOST)
                .setPort(RabbitConstant.PORT)
                .setUserName(RabbitConstant.USERNAME)
                .setPassword(RabbitConstant.PASSWORD)
                .setVirtualHost(RabbitConstant.VIRTUALHOST)
                .build();
        // RabbitMq 数据源配置
        RMQSource<OrderFlinkBO> source = new RMQSource<>(
                // 配置
                config,
                // 队列名称
                RabbitEnum.ORDER_QUEUE.getName(),
                // 序列化模式
                new OrderDeserializer()
        );
        DataStream<OrderFlinkBO> dataStream = env.addSource(source).name("RabbitMQ_Source");
        DataStream<ProductRanking> stream = dataStream.keyBy(OrderFlinkBO::getSkuId)
                // 分区计数窗口 N条消息作为一个窗口
                .countWindow(5)
                .process(new ProductRankingProcess());
                //.apply(new ProductRankingApply());

        // 商品的总排行
        stream.addSink(new ProductRankingSlink<ProductRanking>());

        // 各个地区的商品排行
        /*DataStream<AreaProductRanking> aggregate = dataStream//.keyBy(o -> Tuple2.of(o.getAreaId(), o.getSkuId()))
                .keyBy(new KeySelector<OrderFlinkBO, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> getKey(OrderFlinkBO order) throws Exception {
                        return Tuple2.of(order.getAreaId(), order.getSkuId());
                    }
                })
                .countWindow(5)
                .reduce((o1, o2) -> {
                    o1.setAllCount(o1.getAllCount() + o2.getAllCount());
                    return o1;
                })
                .map(o -> {
                    AreaProductRanking ranking = new AreaProductRanking();
                    ranking.setSkuId(o.getSkuId());
                    ranking.setSkuName(o.getSkuName());
                    ranking.setCount(o.getAllCount());
                    ranking.setAreaId(o.getAreaId());
                    ranking.setAreaName(o.getAreaName());
                    return ranking;
                });
                //.process(new AreaProductProcess());
        aggregate.addSink(new AreaProductSlink<>());*/
        env.execute("Order_Summary_Task");
    }

}
