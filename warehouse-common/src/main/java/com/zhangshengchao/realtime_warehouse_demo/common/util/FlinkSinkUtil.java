package com.zhangshengchao.realtime_warehouse_demo.common.util;

import com.alibaba.fastjson.JSONObject;
import com.zhangshengchao.realtime_warehouse_demo.common.bean.TableProcessDwd;
import com.zhangshengchao.realtime_warehouse_demo.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext;
import org.apache.kafka.clients.producer.ProducerRecord;

// ✅✅✅ 正确的 Doris 包路径（全部修正）
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;

public class FlinkSinkUtil {

    // 原来的方法不动
    public static Sink<String> getKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("zhangshengchao-" + topic + new Random().nextLong())
                .setProperty("transaction.timeout.ms", String.valueOf(15 * 60 * 1000))
                .build();
    }

    // 新增方法：必须带 KafkaSinkContext 参数
    public static Sink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
        return KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(
                            Tuple2<JSONObject, TableProcessDwd> dataWithConfig,
                            KafkaSinkContext context,
                            Long timestamp) {

                        String topic = dataWithConfig.f1.getSinkTable();
                        JSONObject data = dataWithConfig.f0;
                        data.remove("op_type");
                        return new ProducerRecord<>(topic, data.toJSONString().getBytes(StandardCharsets.UTF_8));
                    }
                })
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("zhangshengchao-" + new Random().nextLong())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }

    // ==================== 修复后的正确 Doris Sink ====================
    public static DorisSink<String> getDorisSink(String table, String labelPrefix) {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(Constant.DORIS_FE_NODES)
                        .setTableIdentifier(table)
                        .setUsername("root")
                        .setPassword("")
                        .build()
                )
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        .setLabelPrefix(labelPrefix)
                        .disable2PC()
                        .setBufferCount(3)
                        .setBufferSize(1024 * 1024)
                        .setCheckInterval(3000)
                        .setMaxRetries(3)
                        .setStreamLoadProp(props)
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}