package com.echo.function;

import com.echo.poly.ProductRanking;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class ProductRankingSlink<T> extends RichSinkFunction<ProductRanking> {

    private static JedisPoolConfig config;
    private static JedisPool pool;
    private Jedis jedis;

    private static final String RANK_KEY = "Flink:Order:Ranking";
    private static final String NAMES_KEY = "Flink:Order:Names";

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
        pool = new JedisPool(config, "", 6379, 3000, "E_.", 1);
        jedis = pool.getResource();
    }

    @Override
    public void invoke(ProductRanking ranking, Context context) {
        String skuId = ranking.getSkuId().toString();
        Integer count = ranking.getCount();
        try {
            // 检查商品是否已存在于排行榜中
            if (jedis.zrank(RANK_KEY, skuId) == null) {
                // 添加新商品到排行榜
                jedis.zadd(RANK_KEY, count, skuId);
            } else {
                // 更新商品销量
                jedis.zincrby(RANK_KEY, count, skuId);
            }
            // 存储商品的名称
            if (!jedis.hexists(NAMES_KEY, skuId)) {
                jedis.hset(NAMES_KEY, skuId, ranking.getSkuName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
    }

}
