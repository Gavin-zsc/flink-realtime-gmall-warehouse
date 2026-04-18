package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.CouponReduceStats;
import java.util.List;

public interface CouponStatsService {
    List<CouponReduceStats> getCouponStats(Integer date);
}