package com.atguigu.gmall.service;

import com.atguigu.gmall.mapper.UserChangeCtPerType;
import com.atguigu.gmall.mapper.UserPageCt;
import com.atguigu.gmall.mapper.UserTradeCt;
import com.atguigu.gmall.mapper.UserStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserStatsServiceImpl implements UserStatsService {

    @Autowired
    UserStatsMapper userStatsMapper;

    @Override
    public List<UserChangeCtPerType> getUserChangeCt(Integer date) {
        return userStatsMapper.selectUserChangeCtPerType(date);
    }

    @Override
    public List<UserPageCt> getUvByPage(Integer date) {
        return userStatsMapper.selectUvByPage(date);
    }

    // 你要补充的交易用户统计方法
    @Override
    public List<UserTradeCt> getTradeUserCt(Integer date) {
        return userStatsMapper.selectTradeUserCt(date);
    }
}