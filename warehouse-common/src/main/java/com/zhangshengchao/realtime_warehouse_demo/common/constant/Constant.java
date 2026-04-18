package com.zhangshengchao.realtime_warehouse_demo.common.constant;

/**
 * 电商实时数仓常量类，本项目会用到大量的配置信息，如主题名、Kafka集群地址等，为了减少冗余代码，保证一致性，将这些配置信息统一抽取到常量类中。
 */
public class Constant {
    public static final String KAFKA_BROKERS = "master:9092,slave1:9092,slave2:9092";

    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";

    public static final String MYSQL_HOST = "192.168.145.140";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "123456";
    public static final String HBASE_NAMESPACE = "gmall";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://master:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

    // ==================== 新增的Doris常量 ====================
    public static final String DORIS_FE_NODES = "master:7030,slave1:7030,slave2:7030";
    public static final String DORIS_DATABASE = "gmall2023_realtime";

    // ==================== 解决你报错缺少的常量 ====================
    public static final long TWO_DAY_SECONDS = 2 * 24 * 60 * 60L;
    public static final long ONE_DAY_SECONDS = 24 * 60 * 60L;
    public static final long ONE_HOUR_SECONDS = 60 * 60L;
}