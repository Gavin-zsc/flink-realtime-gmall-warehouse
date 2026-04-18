package com.atguigu.gmall.mapper;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SpuCommodityStats {
    // SPU 名称
    String spuName;
    // 下单金额
    Double orderAmount;
}