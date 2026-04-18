package com.atguigu.gmall.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface ActivityStatsMapper {
    @Select("select sum(order_activity_reduce_amount) activity_reduce_amount,\n" +
            "       sum(order_origin_total_amount)    origin_total_amount,\n" +
            "       sum(order_activity_reduce_amount) /\n" +
            "       sum(order_origin_total_amount)    subsidyRate\n" +
            "from dws_trade_sku_order_window\n" +
            "         partition (par#{date});")
    List<ActivityReduceStats> selectActivityStats(@Param(value = "date")Integer date);
}