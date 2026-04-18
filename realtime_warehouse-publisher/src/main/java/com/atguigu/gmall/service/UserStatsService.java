package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.UserChangeCtPerType;
import com.atguigu.gmall.mapper.UserPageCt;
import com.atguigu.gmall.mapper.UserTradeCt;
import java.util.List;

public interface UserStatsService {
    List<UserChangeCtPerType> getUserChangeCt(Integer date);

    // 页面UV统计
    List<UserPageCt> getUvByPage(Integer date);

    // 交易用户统计（你要加的）
    List<UserTradeCt> getTradeUserCt(Integer date);
}