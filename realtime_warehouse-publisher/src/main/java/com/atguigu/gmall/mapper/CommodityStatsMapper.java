package com.atguigu.gmall.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface CommodityStatsMapper {

    @Select("select oct.trademark_name,\n" +
            "       order_amount,\n" +
            "       refund_count,\n" +
            "       refund_uu_count\n" +
            "from (select trademark_id,\n" +
            "             trademark_name,\n" +
            "             sum(order_amount) order_amount\n" +
            "      from dws_trade_sku_order_window\n" +
            "               partition (par#{date})\n" +
            "      group by trademark_id, trademark_name) oct full outer join\n" +
            "                 (select trademark_id,\n" +
            "                         trademark_name,\n" +
            "                         sum(refund_count)       refund_count,\n" +
            "                         count(distinct user_id) refund_uu_count\n" +
            "                  from dws_trade_trademark_category_user_refund_window\n" +
            "partition (par#{date})\n" +
            "                  group by trademark_id, trademark_name) rct\n" +
            "on oct.trademark_id = rct.trademark_id;")
    List<TrademarkCommodityStats> selectTrademarkStats(@Param("date") Integer date);

    @Select("select trademark_name,\n" +
            "       sum(order_amount) order_amount\n" +
            "from dws_trade_sku_order_window\n" +
            "         partition (par#{date})\n" +
            "group by trademark_id, trademark_name;")
    List<TrademarkOrderAmountPieGraph> selectTmOrderAmtPieGra(@Param("date")Integer date);

    // ===================== 品类统计方法 =====================
    @Select("select oct.category1_name,\n" +
            "       oct.category2_name,\n" +
            "       oct.category3_name,\n" +
            "       order_amount,\n" +
            "       refund_count,\n" +
            "       refund_uu_count\n" +
            "from (select category1_id,\n" +
            "             category1_name,\n" +
            "             category2_id,\n" +
            "             category2_name,\n" +
            "             category3_id,\n" +
            "             category3_name,\n" +
            "             sum(order_amount) order_amount\n" +
            "      from dws_trade_sku_order_window\n" +
            "               partition (par#{date})\n" +
            "      group by category1_id,\n" +
            "               category1_name,\n" +
            "               category2_id,\n" +
            "               category2_name,\n" +
            "               category3_id,\n" +
            "               category3_name) oct full outer join\n" +
            "     (select category1_id,\n" +
            "             category1_name,\n" +
            "             category2_id,\n" +
            "             category2_name,\n" +
            "             category3_id,\n" +
            "             category3_name,\n" +
            "             sum(refund_count)       refund_count,\n" +
            "             count(distinct user_id) refund_uu_count\n" +
            "      from dws_trade_trademark_category_user_refund_window\n" +
            "    partition (par#{date})\n" +
            "      group by category1_id,\n" +
            "               category1_name,\n" +
            "               category2_id,\n" +
            "               category2_name,\n" +
            "               category3_id,\n" +
            "               category3_name) rct\n" +
            "on oct.category1_id = rct.category1_id\n" +
            "    and oct.category2_id = rct.category2_id\n" +
            "    and oct.category3_id = rct.category3_id;")
    List<CategoryCommodityStats> selectCategoryStats(@Param("date") Integer date);

    // ===================== SPU统计方法（你要加的） =====================
    @Select("select spu_name,\n" +
            "       sum(order_amount) order_amount\n" +
            "from dws_trade_sku_order_window\n" +
            "         partition (par#{date})\n" +
            "group by spu_id, spu_name;")
    List<SpuCommodityStats> selectSpuStats(@Param("date") Integer date);
}