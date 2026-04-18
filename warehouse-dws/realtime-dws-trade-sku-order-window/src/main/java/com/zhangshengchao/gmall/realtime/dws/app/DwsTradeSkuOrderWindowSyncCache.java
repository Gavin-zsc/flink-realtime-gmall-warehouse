package com.zhangshengchao.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhangshengchao.realtime_warehouse_demo.common.base.BaseApp;
import com.zhangshengchao.realtime_warehouse_demo.common.bean.TradeSkuOrderBean;
import com.zhangshengchao.realtime_warehouse_demo.common.constant.Constant;
import com.zhangshengchao.realtime_warehouse_demo.common.util.DateFormatUtil;
import com.zhangshengchao.gmall.realtime.dws.function.MapDimFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;

public class DwsTradeSkuOrderWindowSyncCache extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowSyncCache().start(
                10029,
                4,
                "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(stream);
        beanStream = distinctByOrderId(beanStream);
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithoutDims = windowAndAgg(beanStream);
        joinDim(beanStreamWithoutDims);
    }

    private void joinDim(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> skuSteam = stream
                .map(new MapDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getSkuId();
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setSkuName(dim.getString("sku_name"));
                        bean.setSpuId(dim.getString("spu_id"));
                        bean.setTrademarkId(dim.getString("tm_id"));
                        bean.setCategory3Id(dim.getString("category3_id"));
                    }
                });

        SingleOutputStreamOperator<TradeSkuOrderBean> spuStream = skuSteam.map(new MapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getSpuId();
            }

            @Override
            public String getTableName() {
                return "dim_spu_info";
            }

            @Override
            public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                bean.setSpuName(dim.getString("spu_name"));
            }
        });

        SingleOutputStreamOperator<TradeSkuOrderBean> tmStream = spuStream
                .map(new MapDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getTrademarkId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setTrademarkName(dim.getString("tm_name"));
                    }
                });

        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = tmStream.map(new MapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory3Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category3";
            }

            @Override
            public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                bean.setCategory3Name(dim.getString("name"));
                bean.setCategory2Id(dim.getString("category2_id"));
            }
        });

        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = c3Stream.map(new MapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory2Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category2";
            }

            @Override
            public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                bean.setCategory2Name(dim.getString("name"));
                bean.setCategory1Id(dim.getString("category1_id"));
            }
        });

        SingleOutputStreamOperator<TradeSkuOrderBean> c1Stream = c2Stream.map(new MapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory1Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category1";
            }

            @Override
            public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                bean.setCategory1Name(dim.getString("name"));
            }
        });

        c1Stream.print();
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> windowAndAgg(
            SingleOutputStreamOperator<TradeSkuOrderBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(120))
                )
                .keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .reduce(
                        new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) {
                                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                                value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String skuId, Context ctx, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) {
                                TradeSkuOrderBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getEnd()));
                                out.collect(bean);
                            }
                        }
                );
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> distinctByOrderId(
            SingleOutputStreamOperator<TradeSkuOrderBean> beanStream) {
        return beanStream
                .keyBy(TradeSkuOrderBean::getOrderDetailId)
                .process(new KeyedProcessFunction<String, TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private ValueState<TradeSkuOrderBean> lastBeanState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<TradeSkuOrderBean> des = new ValueStateDescriptor<>("lastBean", TradeSkuOrderBean.class);
                        StateTtlConfig conf = StateTtlConfig.newBuilder(Time.seconds(60))
                                .useProcessingTime()
                                .updateTtlOnCreateAndWrite()
                                .neverReturnExpired()
                                .build();
                        des.enableTimeToLive(conf);
                        lastBeanState = getRuntimeContext().getState(des);
                    }

                    @Override
                    public void processElement(TradeSkuOrderBean currentBean, Context ctx, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean lastBean = lastBeanState.value();
                        if (lastBean == null) {
                            out.collect(currentBean);
                        } else {
                            lastBean.setOrderAmount(currentBean.getOrderAmount().subtract(lastBean.getOrderAmount()));
                            lastBean.setOriginalAmount(currentBean.getOriginalAmount().subtract(lastBean.getOriginalAmount()));
                            lastBean.setActivityReduceAmount(currentBean.getActivityReduceAmount().subtract(lastBean.getActivityReduceAmount()));
                            lastBean.setCouponReduceAmount(currentBean.getCouponReduceAmount().subtract(lastBean.getCouponReduceAmount()));
                            out.collect(lastBean);
                        }
                        lastBeanState.update(currentBean);
                    }
                });
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> parseToPojo(DataStreamSource<String> stream) {
        return stream.map(new MapFunction<String, TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean map(String value) {
                JSONObject obj = JSON.parseObject(value);

                return TradeSkuOrderBean.builder()
                        .skuId(obj.getString("sku_id"))
                        .orderDetailId(obj.getString("id"))
                        .originalAmount(obj.getBigDecimal("split_original_amount"))
                        .orderAmount(obj.getBigDecimal("split_total_amount"))
                        .activityReduceAmount(obj.getBigDecimal("split_activity_amount") == null ? new BigDecimal("0.0") : obj.getBigDecimal("split_activity_amount"))
                        .couponReduceAmount(obj.getBigDecimal("split_coupon_amount") == null ? new BigDecimal("0.0") : obj.getBigDecimal("split_coupon_amount"))
                        .ts(obj.getLong("ts") * 1000)
                        .build();
            }
        });
    }
}