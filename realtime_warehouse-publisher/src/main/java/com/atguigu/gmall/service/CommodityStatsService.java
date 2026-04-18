package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.TrademarkCommodityStats;
import com.atguigu.gmall.mapper.TrademarkOrderAmountPieGraph;
import com.atguigu.gmall.mapper.CategoryCommodityStats;
import com.atguigu.gmall.mapper.SpuCommodityStats;
import java.util.List;

public interface CommodityStatsService {
    List<TrademarkCommodityStats> getTrademarkCommodityStatsService(Integer date);

    List<TrademarkOrderAmountPieGraph> getTmOrderAmtPieGra(Integer date);

    // 你要补充的品类统计方法
    List<CategoryCommodityStats> getCategoryStatsService(Integer date);

    // 新增SPU统计方法
    List<SpuCommodityStats> getSpuCommodityStats(Integer date);
}