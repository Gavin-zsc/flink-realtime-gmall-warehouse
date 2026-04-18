package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.TradeStats;
import com.atguigu.gmall.mapper.TradeProvinceOrderCt;
import com.atguigu.gmall.mapper.TradeProvinceOrderAmount;
import com.atguigu.gmall.mapper.TradeStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TradeStatsServiceImpl implements TradeStatsService {

    @Autowired
    TradeStatsMapper tradeStatsMapper;

    @Override
    public Double getTotalAmount(Integer date) {
        return tradeStatsMapper.selectTotalAmount(date);
    }

    @Override
    public List<TradeStats> getTradeStats(Integer date) {
        return tradeStatsMapper.selectTradeStats(date);
    }

    // 新增省份订单数
    @Override
    public List<TradeProvinceOrderCt> getTradeProvinceOrderCt(Integer date) {
        return tradeStatsMapper.selectTradeProvinceOrderCt(date);
    }

    // 新增省份订单金额
    @Override
    public List<TradeProvinceOrderAmount> getTradeProvinceOrderAmount(Integer date) {
        return tradeStatsMapper.selectTradeProvinceOrderAmount(date);
    }
}