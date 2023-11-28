package com.echo.ranking.function;

import com.echo.common.enums.RedisEnum;
import com.echo.ranking.constant.RedisConstant;
import com.echo.ranking.poly.ProductRanking;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class ProductRankingSlink<T> extends RichSinkFunction<ProductRanking> {

    private static JedisPoolConfig config;
    private static JedisPool pool;
    private Jedis jedis;


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
        pool = new JedisPool(config, RedisConstant.HOST, RedisConstant.PORT, RedisConstant.TIMEOUT, RedisConstant.PASSWORD, RedisConstant.DATABASE);
        jedis = pool.getResource();
    }

    @Override
    public void invoke(ProductRanking ranking, Context context) {
        String skuId = ranking.getSkuId().toString();
        Integer count = ranking.getCount();
        try {
            // 检查商品是否已存在于排行榜中
            if (jedis.zrank(RedisEnum.SUM_PRODUCT_RANK.getKey(), skuId) == null) {
                // 添加新商品到排行榜
                jedis.zadd(RedisEnum.SUM_PRODUCT_RANK.getKey(), count, skuId);
            } else {
                // 更新商品销量
                jedis.zincrby(RedisEnum.SUM_PRODUCT_RANK.getKey(), count, skuId);
            }
            // 存储商品的名称
            if (!jedis.hexists(RedisEnum.SUM_PRODUCT_RANK_NAMES.getKey(), skuId)) {
                jedis.hset(RedisEnum.SUM_PRODUCT_RANK_NAMES.getKey(), skuId, ranking.getSkuName());
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
