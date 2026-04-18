package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.ActivityReduceStats;
import com.atguigu.gmall.mapper.ActivityStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ActivityReduceServiceImpl implements ActivityReduceService {

    @Autowired
    private ActivityStatsMapper activityStatsMapper;

    @Override
    public List<ActivityReduceStats> getActivityStats(Integer date) {
        return activityStatsMapper.selectActivityStats(date);
    }
}