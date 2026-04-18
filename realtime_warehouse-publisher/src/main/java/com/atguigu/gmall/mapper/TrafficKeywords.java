package com.atguigu.gmall.mapper;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficKeywords {
    // 关键词
    String keyword;
    // 关键词评分
    Integer keywordScore;
}