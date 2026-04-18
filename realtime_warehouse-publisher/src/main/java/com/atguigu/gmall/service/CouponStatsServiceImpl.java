package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.CouponReduceStats;
import com.atguigu.gmall.mapper.CouponStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CouponStatsServiceImpl implements CouponStatsService {

    @Autowired
    private CouponStatsMapper couponStatsMapper;

    @Override
    public List<CouponReduceStats> getCouponStats(Integer date) {
        return couponStatsMapper.selectCouponStats(date);
    }
}