package com.echo.ranking.function;

import com.echo.ranking.poly.AreaProductRanking;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class AreaProductSlink<T> extends RichSinkFunction<AreaProductRanking> {

    private static JedisPoolConfig config;
    private static JedisPool pool;
    private Jedis jedis;

    private static final String RANK_KEY = "Flink:Product:AreaRanking";

    static {
        // 初始化连接池
        config = new JedisPoolConfig();
        // 最大连接数
        config.setMaxTotal(80);
        // 最大空闲数
        config.setMaxIdle(10);
        // 最小空闲数
        config.setMinIdle(5);
        // 获取连接时的最大等待毫秒数
        config.setMaxWaitMillis(30000);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        pool = new JedisPool(config, "82.156.141.151", 5812, 3000, "E_.d#Z.3L#@.q#k.vE", 1);
        jedis = pool.getResource();
    }

    @Override
    public void invoke(AreaProductRanking ranking, Context context) throws Exception {
        System.out.println("ranking>>>-" + ranking);
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
    }
}
