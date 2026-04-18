package com.atguigu.gmall.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateUtil {

    /**
     * 获取当前日期，格式：yyyyMMdd
     */
    public static int now() {
        return Integer.parseInt(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    }
}