package com.zhangshengchao.realtime_warehouse_demo.common.base;

import com.zhangshengchao.realtime_warehouse_demo.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseApp {

    public abstract void handle(StreamExecutionEnvironment env,
                                DataStreamSource<String> stream);

    public void start(int port, int parallelism, String ckAndGroupId, String topic) {
        // 1. 环境准备
        // 1.1 设置 Hadoop 用户名 → 已经改成你的：hadoop
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 1.2 获取流处理环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.3 设置并行度
        env.setParallelism(parallelism);

        // 1.4 状态后端及检查点相关配置
        env.setStateBackend(new HashMapStateBackend());

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 1.4.4 checkpoint 存储地址 → 已经完全改成你的地址！
        env.getCheckpointConfig().setCheckpointStorage("hdfs://master:9000/stream/ck/" + ckAndGroupId);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        // 1.5 从 Kafka 读取数据
        KafkaSource<String> source = FlinkSourceUtil.getKafkaSource(ckAndGroupId, topic);
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 2. 执行处理逻辑
        handle(env, stream);

        // 3. 执行 Job
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}