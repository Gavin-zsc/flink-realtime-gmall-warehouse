package com.zhangshengchao.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.zhangshengchao.realtime_warehouse_demo.common.bean.TableProcessDim;
import com.zhangshengchao.realtime_warehouse_demo.common.constant.Constant;
import com.zhangshengchao.realtime_warehouse_demo.common.util.HBaseUtil;
import com.zhangshengchao.realtime_warehouse_demo.common.util.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.io.IOException;

@Slf4j
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

    private Connection conn;
    // 1. 定义 Jedis 成员变量
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = HBaseUtil.getHBaseConnection();
        // 2. 初始化 Redis 连接
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConn(conn);
        // 3. 释放 Redis 连接
        RedisUtil.closeJedis(jedis);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> dataWithConfig,
                       Context context) throws Exception {
        JSONObject data = dataWithConfig.f0;
        TableProcessDim tableProcessDim = dataWithConfig.f1;
        String opType = data.getString("op_type");

        // 4. 维度更新或删除时，清除 Redis 缓存
        if ("delete".equals(opType) || "update".equals(opType)) {
            String key = RedisUtil.getKey(
                    tableProcessDim.getSinkTable(),
                    data.getString(tableProcessDim.getSinkRowKey())
            );
            jedis.del(key);
            log.info("清除 Redis 缓存 key: " + key);
        }

        // 执行 HBase 操作
        if ("delete".equals(opType)) {
            delDim(dataWithConfig);
        } else {
            putDim(dataWithConfig);
        }
    }

    private void putDim(Tuple2<JSONObject, TableProcessDim> dataWithConfig) throws IOException {
        JSONObject data = dataWithConfig.f0;
        TableProcessDim tableProcessDim = dataWithConfig.f1;

        String rowKey = data.getString(tableProcessDim.getSinkRowKey());
        log.info("向 HBase 写入数据 dataWithConfig: " + dataWithConfig);

        data.remove("op_type");
        HBaseUtil.putRow(conn,
                Constant.HBASE_NAMESPACE,
                tableProcessDim.getSinkTable(),
                rowKey,
                tableProcessDim.getSinkFamily(),
                data);
    }

    private void delDim(Tuple2<JSONObject, TableProcessDim> dataWithConfig) throws IOException {
        JSONObject data = dataWithConfig.f0;
        TableProcessDim tableProcessDim = dataWithConfig.f1;

        String rowKey = data.getString(tableProcessDim.getSinkRowKey());

        HBaseUtil.delRow(conn,
                Constant.HBASE_NAMESPACE,
                tableProcessDim.getSinkTable(),
                rowKey);
    }
}