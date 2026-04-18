package com.zhangshengchao.realtime_warehouse_demo.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Set;

@Data
@AllArgsConstructor
@Builder
public class TradeTrademarkCategoryUserRefundBean {

    // 窗口起始时间
    private String stt;
    // 窗口结束时间
    private String edt;
    // 当天日期
    private String curDate;

    // 品牌 ID
    private String trademarkId;
    // 品牌名称
    private String trademarkName;

    // 一级品类 ID
    private String category1Id;
    // 一级品类名称
    private String category1Name;
    // 二级品类 ID
    private String category2Id;
    // 二级品类名称
    private String category2Name;
    // 三级品类 ID
    private String category3Id;
    // 三级品类名称
    private String category3Name;

    // 用户 ID
    private String userId;
    // 退单次数
    private Long refundCount;

    // 时间戳（不序列化）
    @JSONField(serialize = false)
    private Long ts;

    // 辅助字段（不输出）
    @JSONField(serialize = false)
    private String skuId;
    @JSONField(serialize = false)
    private Set<String> orderIdSet;
}