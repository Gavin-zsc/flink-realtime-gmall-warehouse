package com.zhangshengchao.gmall.realtime.dws.function;

import com.alibaba.fastjson.JSONObject;
import com.zhangshengchao.realtime_warehouse_demo.common.function.DimFunction;
import com.zhangshengchao.realtime_warehouse_demo.common.util.HBaseUtil;
import com.zhangshengchao.realtime_warehouse_demo.common.util.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

@Slf4j
public abstract class MapDimFunction<T> extends RichMapFunction<T, T> implements DimFunction<T> {

    private Connection hbaseConn;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHBaseConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConn(hbaseConn);
        RedisUtil.closeJedis(jedis);
    }

    @Override
    public T map(T bean) throws Exception {
        // 1. 先去 redis 读取维度
        JSONObject dim = RedisUtil.readDim(jedis, getTableName(), getRowKey(bean));

        if (dim == null) {
            // 2. 如果没有读到, 则去 hbase 读取
            dim = HBaseUtil.getRow(
                    hbaseConn,
                    "gmall",
                    getTableName(),
                    getRowKey(bean),
                    JSONObject.class
            );
            // 3. 并把读到的维度缓存到 redis 中
            RedisUtil.writeDim(jedis, getTableName(), getRowKey(bean), dim);
            log.info("走 hbase: " + getTableName() + "  " + getRowKey(bean));

        } else {
            log.info("走 redis: " + getTableName() + "  " + getRowKey(bean));
        }

        // 补充维度
        addDims(bean, dim);
        return bean;
    }
}