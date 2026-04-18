package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.*;
import java.util.List;

public interface TrafficChannelStatsService {
    // 1. 获取各渠道独立访客数
    List<TrafficUvCt> getUvCt(Integer date);

    // 2. 获取各渠道会话数
    List<TrafficSvCt> getSvCt(Integer date);

    // 3. 获取各渠道会话平均页面浏览数
    List<TrafficPvPerSession> getPvPerSession(Integer date);

    // 4. 获取各渠道会话平均页面访问时长
    List<TrafficDurPerSession> getDurPerSession(Integer date);

    // 5. 获取各渠道跳出率
    List<TrafficUjRate> getUjRate(Integer date);
}