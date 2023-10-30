package com.echo.task;


import com.echo.function.OrderRedisSlink;
import com.echo.function.OrderSumApply;
import com.echo.bo.OrderFlinkBO;
import com.echo.deser.OrderDeserializer;
import com.echo.function.OrderSumProcess;
import com.echo.poly.OrderSummary;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * demo
 */
public class DemoFlink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启定时检查点
        //env.enableCheckpointing(5000);
        // 设置流处理时间特性为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置并行度为 1
        //env.setParallelism(1);

        // 创建RabbitMQ连接配置
        RMQConnectionConfig config = new RMQConnectionConfig.Builder()
                .build();
        // RabbitMq 数据源配置
        RMQSource<OrderFlinkBO> source = new RMQSource<>(
                // 配置
                config,
                // 队列名称
                "order_inform_flink_queue",
                // 序列化模式
                new OrderDeserializer()
        );
        DataStream<OrderFlinkBO> dataStream = env.addSource(source).name("RabbitMQ_Source");
        DataStream<OrderSummary> stream = dataStream.keyBy(OrderFlinkBO::getSkuId)
                // 计数窗口
                .countWindowAll(5)  // N条消息作为一个窗口
                //.trigger(CountTrigger.of(1))
                //.evictor(CountEvictor.of(0))
                .process(new OrderSumProcess());
                //.apply(new OrderSumApply());
        // 滚动窗口
                /*.window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 每10秒为一个窗口
                .trigger(CountTrigger.of(1))    // 收到每个订单消息时触发窗口计算
                .aggregate(new OrderAggregate());*/
        stream.addSink(new OrderRedisSlink<OrderSummary>());
        env.execute("Order_Summary_Task");
    }

}
