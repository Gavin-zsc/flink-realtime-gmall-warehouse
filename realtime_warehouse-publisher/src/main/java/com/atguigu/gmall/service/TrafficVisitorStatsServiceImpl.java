package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.TrafficVisitorStatsPerHour;
import com.atguigu.gmall.mapper.TrafficVisitorStatsMapper;
import com.atguigu.gmall.mapper.TrafficVisitorTypeStats;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficVisitorStatsServiceImpl implements TrafficVisitorStatsService {

    @Autowired
    private TrafficVisitorStatsMapper trafficVisitorStatsMapper;

    // 获取分时流量统计数据
    @Override
    public List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date) {
        return trafficVisitorStatsMapper.selectVisitorStatsPerHr(date);
    }

    // 获取新老访客统计数据
    @Override
    public List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date) {
        return trafficVisitorStatsMapper.selectVisitorTypeStats(date);
    }
}