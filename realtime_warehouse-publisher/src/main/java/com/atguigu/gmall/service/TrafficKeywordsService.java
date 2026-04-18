package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.TrafficKeywords;
import java.util.List;

public interface TrafficKeywordsService {
    List<TrafficKeywords> getKeywords(Integer date);
}