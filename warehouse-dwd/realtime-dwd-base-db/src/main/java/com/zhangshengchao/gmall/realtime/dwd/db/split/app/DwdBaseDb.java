package com.zhangshengchao.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhangshengchao.realtime_warehouse_demo.common.base.BaseApp;
import com.zhangshengchao.realtime_warehouse_demo.common.bean.TableProcessDwd;
import com.zhangshengchao.realtime_warehouse_demo.common.constant.Constant;
import com.zhangshengchao.realtime_warehouse_demo.common.util.FlinkSinkUtil;
import com.zhangshengchao.realtime_warehouse_demo.common.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

@Slf4j
public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(
                10019,
                4,
                "dwd_base_db",
                Constant.TOPIC_DB
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        SingleOutputStreamOperator<TableProcessDwd> configStream = readTableProcess(env);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> dataWithConfigStream = connect(etlStream, configStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> resultStream = deleteNotNeedColumns(dataWithConfigStream);
        writeToKafka(resultStream);
    }

    private void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> resultStream) {
        resultStream.sinkTo(FlinkSinkUtil.getKafkaSink());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> deleteNotNeedColumns(
            SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> dataWithConfigStream) {
        return dataWithConfigStream
                .map(dataWithConfig -> {
                    JSONObject data = dataWithConfig.f0;
                    List<String> columns = new ArrayList<>(Arrays.asList(dataWithConfig.f1.getSinkColumns().split(",")));
                    data.keySet().removeIf(key -> !columns.contains(key));
                    return dataWithConfig;
                });
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> connect(
            SingleOutputStreamOperator<JSONObject> dataStream,
            SingleOutputStreamOperator<TableProcessDwd> configStream) {

        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor =
                new MapStateDescriptor<>("table_process_dwd", String.class, TableProcessDwd.class);

        BroadcastStream<TableProcessDwd> broadcastStream = configStream.broadcast(mapStateDescriptor);

        return dataStream
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {

                    private Map<String, TableProcessDwd> map;

                    // ====================== 你说的正确修改：open 必须捕获异常 ======================
                    @Override
                    public void open(Configuration parameters) {
                        map = new HashMap<>();
                        Connection mysqlConn = null;
                        try {
                            mysqlConn = JdbcUtil.getMysqlConnection();
                            List<TableProcessDwd> tableProcessDwdList = JdbcUtil.queryList(
                                    mysqlConn,
                                    "select * from gmall2023_config.table_process_dwd",
                                    TableProcessDwd.class,
                                    true
                            );

                            for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
                                String key = getKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());
                                map.put(key, tableProcessDwd);
                            }
                        } catch (Exception e) {
                            log.error("初始化配置表失败", e);
                            throw new RuntimeException("初始化配置表失败", e);
                        } finally {
                            if (mysqlConn != null) {
                                try {
                                    mysqlConn.close();
                                } catch (Exception e) {
                                    log.warn("关闭 MySQL 连接失败", e);
                                }
                            }
                        }
                    }

                    // ====================== 无需修改：已声明 throws Exception ======================
                    @Override
                    public void processElement(JSONObject jsonObj,
                                               ReadOnlyContext context,
                                               Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcessDwd> state = context.getBroadcastState(mapStateDescriptor);
                        String key = getKey(jsonObj.getString("table"), jsonObj.getString("type"));
                        TableProcessDwd tableProcessDwd = state.get(key);

                        if (tableProcessDwd == null) {
                            tableProcessDwd = map.get(key);
                            if (tableProcessDwd != null) {
                                log.info("在 map 中查找到 " + key);
                            }
                        } else {
                            log.info("在状态中查找到 " + key);
                        }

                        if (tableProcessDwd != null) {
                            JSONObject data = jsonObj.getJSONObject("data");
                            out.collect(Tuple2.of(data, tableProcessDwd));
                        }
                    }

                    // ====================== 无需修改：已声明 throws Exception ======================
                    @Override
                    public void processBroadcastElement(TableProcessDwd tableProcessDwd,
                                                        Context context,
                                                        Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                        BroadcastState<String, TableProcessDwd> state = context.getBroadcastState(mapStateDescriptor);
                        String key = getKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());

                        if ("d".equals(tableProcessDwd.getOp())) {
                            state.remove(key);
                            map.remove(key);
                        } else {
                            state.put(key, tableProcessDwd);
                        }
                    }

                    private String getKey(String table, String type) {
                        return table + ":" + type;
                    }
                });
    }

    private SingleOutputStreamOperator<TableProcessDwd> readTableProcess(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall2023_config")
                .tableList("gmall2023_config.table_process_dwd")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source")
                .setParallelism(1)
                .map(value -> {
                    JSONObject obj = JSON.parseObject(value);
                    String op = obj.getString("op");
                    TableProcessDwd tp;
                    if ("d".equals(op)) {
                        tp = obj.getObject("before", TableProcessDwd.class);
                    } else {
                        tp = obj.getObject("after", TableProcessDwd.class);
                    }
                    tp.setOp(op);
                    return tp;
                })
                .setParallelism(1);
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .filter(value -> {
                    try {
                        JSONObject obj = JSON.parseObject(value);
                        String db = obj.getString("database");
                        String type = obj.getString("type");
                        String data = obj.getString("data");

                        return "gmall".equals(db)
                                && ("insert".equals(type) || "update".equals(type))
                                && data != null
                                && data.length() > 2;

                    } catch (Exception e) {
                        log.warn("非标准JSON格式数据：" + value);
                        return false;
                    }
                })
                .map(JSON::parseObject);
    }
}