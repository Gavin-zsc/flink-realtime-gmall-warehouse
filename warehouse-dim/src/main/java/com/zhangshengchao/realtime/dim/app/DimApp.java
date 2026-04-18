package com.zhangshengchao.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhangshengchao.realtime.dim.function.HBaseSinkFunction;
import com.zhangshengchao.realtime_warehouse_demo.common.base.BaseApp;
import com.zhangshengchao.realtime_warehouse_demo.common.bean.TableProcessDim;
import com.zhangshengchao.realtime_warehouse_demo.common.constant.Constant;
import com.zhangshengchao.realtime_warehouse_demo.common.util.HBaseUtil;
import com.zhangshengchao.realtime_warehouse_demo.common.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

@Slf4j
public class DimApp extends BaseApp {

    public static void main(String[] args) {
        new DimApp().start(
                10001,
                4,
                "dim_app",
                Constant.TOPIC_DB
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        log.info("==================== DimApp 启动成功 ====================");

        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        SingleOutputStreamOperator<TableProcessDim> configStream = readTableProcess(env);
        configStream = createHBaseTable(configStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDataToTpStream = connect(etlStream, configStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> resultStream = deleteNotNeedColumns(dimDataToTpStream);
        writeToHBase(resultStream);
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .map(value -> {
                    log.info("【KAFKA原始数据】: {}", value);
                    return value;
                })
                .filter(value -> {
                    try {
                        JSONObject jsonObj = JSON.parseObject(value);
                        String db = jsonObj.getString("database");
                        String type = jsonObj.getString("type");
                        String data = jsonObj.getString("data");

                        boolean pass = "business_db".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2;

                        log.info("【KAFKA过滤】db={}, type={}, dataLen={}, 结果={}",
                                db, type, (data == null ? 0 : data.length()), pass);
                        return pass;

                    } catch (Exception e) {
                        log.warn("【KAFKA异常】不是json: " + value);
                        return false;
                    }
                })
                .map((MapFunction<String, JSONObject>) JSON::parseObject);
    }

    private SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("business_db_config")
                .tableList("business_db_config.table_process_dim")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        return env
                .fromSource(mySqlSource, WatermarkStrategy.forMonotonousTimestamps(), "cdc-source")
                .setParallelism(1)
                .map(value -> {
                    log.info("【CDC原始数据】: {}", value);
                    return value;
                })
                .map(value -> {
                    JSONObject obj = JSON.parseObject(value);
                    String op = obj.getString("op");
                    TableProcessDim tableProcessDim;
                    if ("d".equals(op)) {
                        tableProcessDim = obj.getObject("before", TableProcessDim.class);
                    } else {
                        tableProcessDim = obj.getObject("after", TableProcessDim.class);
                    }
                    tableProcessDim.setOp(op);
                    log.info("【CDC解析完成】op={}, 表={}", op, tableProcessDim.getSourceTable());
                    return tableProcessDim;
                })
                .setParallelism(1);
    }

    private SingleOutputStreamOperator<TableProcessDim> createHBaseTable(
            SingleOutputStreamOperator<TableProcessDim> tpStream) {
        return tpStream.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {

            private Connection hbaseConn;

            @Override
            public void open(Configuration parameters) {
                try {
                    log.info("【HBase】准备建立连接");
                    hbaseConn = HBaseUtil.getHBaseConnection();
                    log.info("【HBase】连接成功: {}", hbaseConn != null);
                } catch (IOException e) {
                    log.error("【HBase】连接失败", e);
                    throw new RuntimeException("HBase连接失败", e);
                }
            }

            @Override
            public void close() {
                try {
                    HBaseUtil.closeHBaseConn(hbaseConn);
                } catch (IOException e) {
                    log.error("【HBase】关闭连接失败", e);
                }
            }

            @Override
            public TableProcessDim map(TableProcessDim tableProcessDim) {
                log.info("【HBase处理配置】op={}, 源表={}, 目标表={}",
                        tableProcessDim.getOp(),
                        tableProcessDim.getSourceTable(),
                        tableProcessDim.getSinkTable());

                String op = tableProcessDim.getOp();
                if (!"d".equals(op)) {
                    try {
                        createTable(tableProcessDim);
                    } catch (IOException e) {
                        log.error("【HBase建表失败】{}", tableProcessDim.getSinkTable(), e);
                        throw new RuntimeException("HBase建表失败: " + tableProcessDim.getSinkTable(), e);
                    }
                }
                return tableProcessDim;
            }

            private void createTable(TableProcessDim tableProcessDim) throws IOException {
                log.info("【HBase建表】namespace={}, table={}, family={}",
                        Constant.HBASE_NAMESPACE,
                        tableProcessDim.getSinkTable(),
                        tableProcessDim.getSinkFamily());

                HBaseUtil.createHBaseTable(
                        hbaseConn,
                        Constant.HBASE_NAMESPACE,
                        tableProcessDim.getSinkTable(),
                        tableProcessDim.getSinkFamily()
                );
                log.info("【HBase建表成功】{}", tableProcessDim.getSinkTable());
            }
        }).setParallelism(1);
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(
            SingleOutputStreamOperator<JSONObject> dataStream,
            SingleOutputStreamOperator<TableProcessDim> configStream) {

        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<>("table_process_dim", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStream = configStream.broadcast(mapStateDescriptor);

        return dataStream
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {

                    private HashMap<String, TableProcessDim> map;

                    @Override
                    public void open(Configuration parameters) {
                        map = new HashMap<>();
                        log.info("【初始化】从MySQL预加载配置表...");
                        java.sql.Connection mysqlConn = null;
                        try {
                            mysqlConn = JdbcUtil.getMysqlConnection();
                            List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mysqlConn,
                                    "select * from business_db_config.table_process_dim",
                                    TableProcessDim.class,
                                    true
                            );

                            log.info("【初始化】加载到 {} 条配置", tableProcessDimList.size());

                            for (TableProcessDim tableProcessDim : tableProcessDimList) {
                                String key = tableProcessDim.getSourceTable();
                                map.put(key, tableProcessDim);
                                log.info("【初始化加载】源表={} → 目标表={}", key, tableProcessDim.getSinkTable());
                            }
                        } catch (Exception e) {
                            log.error("【初始化加载配置失败】", e);
                            throw new RuntimeException("初始化加载配置失败", e);
                        } finally {
                            try {
                                JdbcUtil.closeConnection(mysqlConn);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcessDim tableProcessDim,
                                                        Context context,
                                                        Collector<Tuple2<JSONObject, TableProcessDim>> out) {
                        BroadcastState<String, TableProcessDim> state = context.getBroadcastState(mapStateDescriptor);
                        String key = tableProcessDim.getSourceTable();
                        if ("d".equals(tableProcessDim.getOp())) {
                            try {
                                state.remove(key);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            map.remove(key);
                        } else {
                            try {
                                state.put(key, tableProcessDim);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            map.put(key, tableProcessDim);
                        }
                    }

                    @Override
                    public void processElement(JSONObject jsonObj,
                                               ReadOnlyContext context,
                                               Collector<Tuple2<JSONObject, TableProcessDim>> out) {
                        String key = jsonObj.getString("table");
                        TableProcessDim tableProcessDim = map.get(key);

                        if (tableProcessDim != null) {
                            JSONObject data = jsonObj.getJSONObject("data");
                            data.put("op_type", jsonObj.getString("type"));
                            out.collect(Tuple2.of(data, tableProcessDim));
                        }
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<JSONObject, TableProcessDim>>() {}));
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> deleteNotNeedColumns(
            SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDataToTpStream) {
        return dimDataToTpStream
                .map(dataWithConfig -> {
                    JSONObject data = dataWithConfig.f0;
                    List<String> columns = new ArrayList<>(Arrays.asList(dataWithConfig.f1.getSinkColumns().split(",")));
                    columns.add("op_type");
                    data.keySet().removeIf(key -> !columns.contains(key));
                    log.info("【最终数据】写出到HBase: {}", data);
                    return dataWithConfig;
                });
    }

    private void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> resultStream) {
        resultStream.addSink(new HBaseSinkFunction());
    }

}