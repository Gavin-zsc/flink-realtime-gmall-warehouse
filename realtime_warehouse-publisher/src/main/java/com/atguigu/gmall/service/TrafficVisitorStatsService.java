package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.TrafficVisitorStatsPerHour;
import com.atguigu.gmall.mapper.TrafficVisitorTypeStats;
import java.util.List;

public interface TrafficVisitorStatsService {
    // 获取分时流量数据
    List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(Integer date);

    // 获取新老访客统计数据
    List<TrafficVisitorTypeStats> getVisitorTypeStats(Integer date);
}