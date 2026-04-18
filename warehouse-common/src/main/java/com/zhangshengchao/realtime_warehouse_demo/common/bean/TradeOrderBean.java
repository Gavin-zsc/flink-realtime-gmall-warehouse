package com.zhangshengchao.realtime_warehouse_demo.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeOrderBean {
    // 窗口起始时间
    private String stt;
    // 窗口关闭时间
    private String edt;
    // 当天日期
    private String curDate;
    // 下单独立用户数
    private Long orderUniqueUserCount;
    // 下单新用户数
    private Long orderNewUserCount;
    // 时间戳
    @JSONField(serialize = false)
    private Long ts;
}