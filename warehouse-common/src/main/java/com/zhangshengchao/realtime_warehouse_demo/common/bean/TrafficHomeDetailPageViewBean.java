package com.zhangshengchao.realtime_warehouse_demo.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficHomeDetailPageViewBean {

    // 窗口起始时间
    private String stt;
    // 窗口结束时间
    private String edt;
    // 当天日期
    private String curDate;
    // 首页独立访客数
    private Long homeUvCt;
    // 商品详情页独立访客数
    private Long goodDetailUvCt;
    // 时间戳
    @JSONField(serialize = false)
    private Long ts;
}