package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.CategoryCommodityStats;
import com.atguigu.gmall.mapper.TrademarkCommodityStats;
import com.atguigu.gmall.mapper.TrademarkOrderAmountPieGraph;
import com.atguigu.gmall.mapper.SpuCommodityStats;
import com.atguigu.gmall.mapper.CommodityStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CommodityStatsServiceImpl implements CommodityStatsService {

    @Autowired
    private CommodityStatsMapper commodityStatsMapper;

    @Override
    public List<TrademarkCommodityStats> getTrademarkCommodityStatsService(Integer date) {
        return commodityStatsMapper.selectTrademarkStats(date);
    }

    @Override
    public List<TrademarkOrderAmountPieGraph> getTmOrderAmtPieGra(Integer date) {
        return commodityStatsMapper.selectTmOrderAmtPieGra(date);
    }

    // 你要补充的品类统计实现方法
    @Override
    public List<CategoryCommodityStats> getCategoryStatsService(Integer date) {
        return commodityStatsMapper.selectCategoryStats(date);
    }

    // 新增SPU统计实现方法
    @Override
    public List<SpuCommodityStats> getSpuCommodityStats(Integer date) {
        return commodityStatsMapper.selectSpuStats(date);
    }
}