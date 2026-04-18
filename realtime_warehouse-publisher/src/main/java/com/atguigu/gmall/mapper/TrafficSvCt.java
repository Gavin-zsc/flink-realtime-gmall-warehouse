package com.atguigu.gmall.mapper;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficSvCt {
    // 渠道
    String ch;
    // 会话数
    Integer svCt;
}