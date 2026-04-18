package com.zhangshengchao.realtime_warehouse_demo.common.base;

import com.zhangshengchao.realtime_warehouse_demo.common.constant.Constant;
import com.zhangshengchao.realtime_warehouse_demo.common.util.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseSQLApp {

    public abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv);

    public void start(int port, int parallelism, String ck) {
        // 你确认的 HDFS 操作用户：hadoop
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        // 状态后端
        env.setStateBackend(new HashMapStateBackend());

        // Checkpoint 配置
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 你确认的 HDFS 地址 + 路径
        env.getCheckpointConfig().setCheckpointStorage("hdfs://master:9000/gmall2023/sql/" + ck);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        handle(env, tEnv);
    }

    // 读取 Kafka 的 topic_db
    public void readOdsDb(StreamTableEnvironment tEnv, String groupId) {
        tEnv.executeSql("create table topic_db (" +
                "  `database` string, " +
                "  `table` string, " +
                "  `type` string, " +
                "  `data` map<string, string>, " +
                "  `old` map<string, string>, " +
                "  `ts` bigint, " +
                "  `pt` as proctime(), " +
                "  et as to_timestamp_ltz(ts, 0), " +
                "  watermark for et as et - interval '3' second " +
                ")" + SQLUtil.getKafkaDDLSource(groupId, Constant.TOPIC_DB));
    }

    // 读取 HBase 字典表
    public void readBaseDic(StreamTableEnvironment tEnv) {
        tEnv.executeSql(
                "create table base_dic (" +
                        " dic_code string," +
                        " info row<dic_name string>, " +
                        " primary key (dic_code) not enforced " +
                        ") WITH (" +
                        " 'connector' = 'hbase-2.2'," +
                        " 'table-name' = 'gmall:dim_base_dic'," +
                        // 你确认的 ZK 节点
                        " 'zookeeper.quorum' = 'master:2181,slave1:2181,slave2:2181', " +
                        " 'lookup.cache' = 'PARTIAL', " +
                        " 'lookup.async' = 'true', " +
                        " 'lookup.partial-cache.max-rows' = '20', " +
                        " 'lookup.partial-cache.expire-after-access' = '2 hour' " +
                        ")");
    }
}