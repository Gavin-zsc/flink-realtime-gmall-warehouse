package com.atguigu.gmall.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface CouponStatsMapper {
    @Select("select sum(order_coupon_reduce_amount) coupon_reduce_amount,\n" +
            "       sum(order_origin_total_amount)  origin_total_amount,\n" +
            "       sum(order_coupon_reduce_amount) /\n" +
            "       sum(order_origin_total_amount)  coupon_subsidy_rate\n" +
            "from dws_trade_sku_order_window\n" +
            "         partition (par#{date});")
    List<CouponReduceStats> selectCouponStats(@Param("date")Integer date);
}