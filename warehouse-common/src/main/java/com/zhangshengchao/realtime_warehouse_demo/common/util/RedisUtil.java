package com.zhangshengchao.realtime_warehouse_demo.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhangshengchao.realtime_warehouse_demo.common.constant.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisUtil {

    // ==================== 同步客户端（Jedis）====================
    private final static JedisPool pool;

    static {
        GenericObjectPoolConfig<Jedis> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(300);
        config.setMaxIdle(10);
        config.setMinIdle(2);

        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        config.setMaxWaitMillis(10 * 1000);

        pool = new JedisPool(config, "master", 6379);
    }

    public static Jedis getJedis() {
        Jedis jedis = pool.getResource();
        jedis.select(4);
        return jedis;
    }

    public static JSONObject readDim(Jedis jedis, String tableName, String id) {
        String key = getKey(tableName, id);
        String jsonStr = jedis.get(key);
        if (jsonStr != null) {
            return JSON.parseObject(jsonStr);
        }
        return null;
    }

    public static void writeDim(Jedis jedis, String tableName, String id, JSONObject dim) {
        // 修复1：Jedis的setex方法参数顺序为 key, 过期时间(秒), 值
        jedis.setex(getKey(tableName, id), Constant.TWO_DAY_SECONDS, dim.toJSONString());
    }

    public static String getKey(String tableName, String id) {
        return tableName + ":" + id;
    }

    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    // ==================== 异步客户端（Lettuce）====================
    /**
     * 获取 Redis 异步连接
     */
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        // 修复2：用try-with-resources自动关闭RedisClient，消除资源泄漏警告
        try (RedisClient redisClient = RedisClient.create("redis://master:6379/2")) {
            return redisClient.connect();
        }
    }

    /**
     * 关闭 Redis 异步连接
     */
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> redisAsyncConn) {
        if (redisAsyncConn != null) {
            redisAsyncConn.close();
        }
    }

    /**
     * 异步从 Redis 读取维度
     */
    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> redisAsyncConn,
                                          String tableName,
                                          String id) {
        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();
        String key = getKey(tableName, id);
        try {
            String json = asyncCommand.get(key).get();
            if (json != null) {
                return JSON.parseObject(json);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * 异步写入维度到 Redis
     */
    public static void writeDimAsync(StatefulRedisConnection<String, String> redisAsyncConn,
                                     String tableName,
                                     String id,
                                     JSONObject dim) {
        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();
        String key = getKey(tableName, id);
        // Lettuce的setex方法参数顺序为 key, 过期时间(秒), 值，与Jedis一致，无需修改
        asyncCommand.setex(key, Constant.TWO_DAY_SECONDS, dim.toJSONString());
    }
}