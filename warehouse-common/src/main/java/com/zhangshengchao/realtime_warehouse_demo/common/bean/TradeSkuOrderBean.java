package com.zhangshengchao.realtime_warehouse_demo.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeSkuOrderBean {
    @JSONField(serialize = false)
    private String orderDetailId;
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
    // sku_id
    private String skuId;
    // sku 名称
    private String skuName;
    // spu_id
    private String spuId;
    // spu 名称
    private String spuName;
    // 原始金额
    private BigDecimal originalAmount;
    // 活动减免金额
    private BigDecimal activityReduceAmount;
    // 优惠券减免金额
    private BigDecimal couponReduceAmount;
    // 下单金额
    private BigDecimal orderAmount;
    // 时间戳
    @JSONField(serialize = false)
    private Long ts;
}