package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.TradeStats;
import com.atguigu.gmall.mapper.TradeProvinceOrderCt;
import com.atguigu.gmall.mapper.TradeProvinceOrderAmount;
import java.util.List;

public interface TradeStatsService {
    Double getTotalAmount(Integer date);

    List<TradeStats> getTradeStats(Integer date);

    // 新增省份订单数统计
    List<TradeProvinceOrderCt> getTradeProvinceOrderCt(Integer date);

    // 新增省份订单金额统计
    List<TradeProvinceOrderAmount> getTradeProvinceOrderAmount(Integer date);
}