package com.zhangshengchao.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhangshengchao.realtime_warehouse_demo.common.base.BaseApp;
import com.zhangshengchao.realtime_warehouse_demo.common.bean.TradeProvinceOrderBean;
import com.zhangshengchao.realtime_warehouse_demo.common.constant.Constant;
import com.zhangshengchao.realtime_warehouse_demo.common.function.AsyncDimFunction;
import com.zhangshengchao.realtime_warehouse_demo.common.function.DorisMapFunction;
import com.zhangshengchao.realtime_warehouse_demo.common.util.DateFormatUtil;
import com.zhangshengchao.realtime_warehouse_demo.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(
                10020,
                4,
                "dws_trade_province_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TradeProvinceOrderBean> reducedStream = stream
                .map(new MapFunction<String, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);

                        HashSet<String> set = new HashSet<>();
                        set.add(obj.getString("order_id"));

                        return TradeProvinceOrderBean.builder()
                                .orderDetailId(obj.getString("id"))
                                .orderAmount(obj.getBigDecimal("split_total_amount"))
                                .provinceId(obj.getString("province_id"))
                                .ts(obj.getLong("ts") * 1000)
                                .orderIdSet(set)
                                .build();
                    }
                })
                .keyBy(TradeProvinceOrderBean::getOrderDetailId)
                .process(new KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>() {

                    private ValueState<Boolean> isFirstState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isFirstState = getRuntimeContext().getState(new ValueStateDescriptor<>("isFirst", Boolean.class));
                    }

                    @Override
                    public void processElement(TradeProvinceOrderBean value,
                                               Context ctx,
                                               Collector<TradeProvinceOrderBean> out) throws Exception {
                        if (isFirstState.value() == null) {
                            isFirstState.update(true);
                            out.collect(value);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(120L))
                )
                .keyBy(TradeProvinceOrderBean::getProvinceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TradeProvinceOrderBean>() {
                            @Override
                            public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1,
                                                                 TradeProvinceOrderBean value2) throws Exception {
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String provinceId,
                                                Context ctx,
                                                Iterable<TradeProvinceOrderBean> elements,
                                                Collector<TradeProvinceOrderBean> out) throws Exception {
                                TradeProvinceOrderBean bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                                bean.setOrderCount((long) bean.getOrderIdSet().size());
                                out.collect(bean);
                            }
                        }
                );

        AsyncDataStream
                .unorderedWait(
                        reducedStream,
                        new AsyncDimFunction<TradeProvinceOrderBean>() {
                            @Override
                            public String getRowKey(TradeProvinceOrderBean bean) {
                                return bean.getProvinceId();
                            }

                            @Override
                            public String getTableName() {
                                return "dim_base_province";
                            }

                            @Override
                            public void addDims(TradeProvinceOrderBean bean,
                                                JSONObject dim) {
                                bean.setProvinceName(dim.getString("name"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                )
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(
                        Constant.DORIS_DATABASE + ".dws_trade_province_order_window",
                        "dws_trade_province_order_window"
                ));
    }
}