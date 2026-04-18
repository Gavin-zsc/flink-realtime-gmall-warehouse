package com.zhangshengchao.realtime_warehouse_demo.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    private String stt;
    // 窗口终止时间
    private String edt;
    // 当天日期
    private String curDate;
    // 注册用户数
    private Long registerCt;
}