package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.ActivityReduceStats;
import java.util.List;

public interface ActivityReduceService {
    List<ActivityReduceStats> getActivityStats(Integer date);
}