package com.zhangshengchao.realtime_warehouse_demo.common.function;

import com.alibaba.fastjson.JSONObject;
import com.zhangshengchao.realtime_warehouse_demo.common.util.HBaseUtil;
import com.zhangshengchao.realtime_warehouse_demo.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> implements DimFunction<T> {

    private StatefulRedisConnection<String, String> redisAsyncConn;
    private AsyncConnection hBaseAsyncConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
        HBaseUtil.closeAsyncHbaseConnection(hBaseAsyncConn);
    }

    @Override
    public void asyncInvoke(T bean, ResultFuture<T> resultFuture) {

        CompletableFuture
                .supplyAsync(new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        return RedisUtil.readDimAsync(redisAsyncConn, getTableName(), getRowKey(bean));
                    }
                })
                .thenApplyAsync(new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimFromRedis) {
                        JSONObject dim = dimFromRedis;
                        if (dim == null) {
                            dim = HBaseUtil.readDimAsync(hBaseAsyncConn, "gmall", getTableName(), getRowKey(bean));
                            RedisUtil.writeDimAsync(redisAsyncConn, getTableName(), getRowKey(bean), dim);
                            log.info("走的是 hbase " + getTableName() + "  " + getRowKey(bean));
                        } else {
                            log.info("走的是 redis " + getTableName() + "  " + getRowKey(bean));
                        }
                        return dim;
                    }
                })
                .thenAccept(new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dim) {
                        addDims(bean, dim);
                        resultFuture.complete(Collections.singleton(bean));
                    }
                });
    }
}