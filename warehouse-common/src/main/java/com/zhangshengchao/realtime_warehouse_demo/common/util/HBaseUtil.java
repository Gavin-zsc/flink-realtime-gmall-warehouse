package com.zhangshengchao.realtime_warehouse_demo.common.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

@Slf4j
public class HBaseUtil {

    // ==================== 同步连接 ====================
    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "master");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return ConnectionFactory.createConnection(conf);
    }

    public static void closeHBaseConn(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    // ==================== 建表 / 删表 ====================
    public static void createHBaseTable(Connection hbaseConn,
                                        String nameSpace,
                                        String table,
                                        String family) throws IOException {
        Admin admin = hbaseConn.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace, table);

        if (admin.tableExists(tableName)) {
            log.info(nameSpace + ":" + table + " 表已存在，无需创建");
            return;
        }

        ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder.of(family);
        TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(cfDesc)
                .build();

        admin.createTable(desc);
        admin.close();
        log.info(nameSpace + ":" + table + " 建表成功");
    }

    public static void dropHBaseTable(Connection hbaseConn,
                                      String nameSpace,
                                      String table) throws IOException {
        Admin admin = hbaseConn.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace, table);

        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        admin.close();
        log.info(nameSpace + ":" + table + " 删除成功");
    }

    // ==================== 写入 / 删除数据 ====================
    public static void putRow(Connection conn,
                              String nameSpace,
                              String table,
                              String rowKey,
                              String family,
                              JSONObject data) throws IOException {
        TableName tableName = TableName.valueOf(nameSpace, table);
        Table t = conn.getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowKey));
        for (String key : data.keySet()) {
            String value = data.getString(key);
            if (value != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(value));
            }
        }

        t.put(put);
        t.close();
    }

    public static void delRow(Connection conn,
                              String nameSpace,
                              String table,
                              String rowKey) throws IOException {
        TableName tableName = TableName.valueOf(nameSpace, table);
        Table t = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        t.delete(delete);
        t.close();
    }

    // ==================== 同步查询（封装成Bean）====================
    public static <T> T getRow(Connection hbaseConn,
                               String nameSpace,
                               String table,
                               String rowKey,
                               Class<T> tClass,
                               boolean... isUnderlineToCamel) {
        boolean defaultIsUToC = false;

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        try (Table tableObj = hbaseConn.getTable(TableName.valueOf(nameSpace, table))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = tableObj.get(get);

            List<Cell> cells = result.listCells();
            T t = tClass.newInstance();

            if (cells != null && !cells.isEmpty()) {
                for (Cell cell : cells) {
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if (defaultIsUToC) {
                        key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key);
                    }
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    BeanUtils.setProperty(t, key, value);
                }
            }
            return t;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ==================== 异步连接（新增）====================
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "master");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // ==================== 异步查询维度（新增）====================
    public static JSONObject readDimAsync(AsyncConnection hBaseAsyncConn,
                                          String nameSpace,
                                          String tableName,
                                          String rowKey) {
        AsyncTable<AdvancedScanResultConsumer> asyncTable = hBaseAsyncConn
                .getTable(TableName.valueOf(nameSpace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            JSONObject dim = new JSONObject();

            if (cells != null && !cells.isEmpty()) {
                for (Cell cell : cells) {
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    dim.put(key, value);
                }
            }
            return dim;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}