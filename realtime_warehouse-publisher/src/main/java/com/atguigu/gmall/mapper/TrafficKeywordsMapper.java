package com.atguigu.gmall.mapper;

import com.atguigu.gmall.mapper.TrafficKeywords;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TrafficKeywordsMapper {
    @Select("select keyword,\n" +
            "       sum(keyword_count * case source\n" +
            "                               when 'SEARCH' then 10\n" +
            "                               when 'ORDER' then 5\n" +
            "                               when 'CART' then 2\n" +
            "                               when 'CLICK' then 1\n" +
            "                               else 0 end\n" +
            "           ) keyword_score\n" +
            "from dws_traffic_source_keyword_page_view_window\n" +
            "         partition (par#{date})\n" +
            "group by keyword\n" +
            "order by keyword_score desc;")
    List<TrafficKeywords> selectKeywords(@Param(value = "date") Integer date);
}