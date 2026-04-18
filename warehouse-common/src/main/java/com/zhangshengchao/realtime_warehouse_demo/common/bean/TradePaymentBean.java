package com.zhangshengchao.realtime_warehouse_demo.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TradePaymentBean {
    // 窗口起始时间
    private String stt;
    // 窗口终止时间
    private String edt;
    // 当天日期
    private String curDate;
    // 支付成功独立用户数
    private Long paymentSucUniqueUserCount;
    // 支付成功新用户数
    private Long paymentSucNewUserCount;
    // 时间戳
    @JSONField(serialize = false)
    private Long ts;
}