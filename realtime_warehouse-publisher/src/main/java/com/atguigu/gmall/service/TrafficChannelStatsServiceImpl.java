package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficChannelStatsServiceImpl implements TrafficChannelStatsService {

    // 自动装载 Mapper 接口实现类
    @Autowired
    TrafficChannelStatsMapper trafficChannelStatsMapper;

    // 1. 获取各渠道独立访客数
    @Override
    public List<TrafficUvCt> getUvCt(Integer date) {
        return trafficChannelStatsMapper.selectUvCt(date);
    }

    // 2. 获取各渠道会话数
    @Override
    public List<TrafficSvCt> getSvCt(Integer date) {
        return trafficChannelStatsMapper.selectSvCt(date);
    }

    // 3. 获取各渠道会话平均页面浏览数
    @Override
    public List<TrafficPvPerSession> getPvPerSession(Integer date) {
        return trafficChannelStatsMapper.selectPvPerSession(date);
    }

    // 4. 获取各渠道会话平均页面访问时长
    @Override
    public List<TrafficDurPerSession> getDurPerSession(Integer date) {
        return trafficChannelStatsMapper.selectDurPerSession(date);
    }

    // 5. 获取各渠道跳出率
    @Override
    public List<TrafficUjRate> getUjRate(Integer date) {
        return trafficChannelStatsMapper.selectUjRate(date);
    }
}