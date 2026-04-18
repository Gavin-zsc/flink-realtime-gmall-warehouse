package com.atguigu.gmall.controller;

import com.atguigu.gmall.mapper.*;
import com.atguigu.gmall.service.TrafficChannelStatsService;
import com.atguigu.gmall.service.TrafficKeywordsService;
import com.atguigu.gmall.service.TrafficVisitorStatsService;
import com.atguigu.gmall.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/gmall/realtime/traffic")
public class TrafficController {

    // 自动装载渠道流量统计服务实现类
    @Autowired
    private TrafficChannelStatsService trafficChannelStatsService;

    // 自动装载访客状态统计服务实现类
    @Autowired
    private TrafficVisitorStatsService trafficVisitorStatsService;

    // 自动装载关键词统计服务
    @Autowired
    private TrafficKeywordsService trafficKeywordsService;

    // 1. 独立访客请求拦截方法
    @RequestMapping("/uvCt")
    public String getUvCt(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TrafficUvCt> trafficUvCtList = trafficChannelStatsService.getUvCt(date);
        if (trafficUvCtList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder uvCtValues = new StringBuilder("[");

        for (int i = 0; i < trafficUvCtList.size(); i++) {
            TrafficUvCt trafficUvCt = trafficUvCtList.get(i);
            String ch = trafficUvCt.getCh();
            Integer uvCt = trafficUvCt.getUvCt();

            categories.append("\"").append(ch).append("\"");
            uvCtValues.append("\"").append(uvCt).append("\"");

            if (i < trafficUvCtList.size() - 1) {
                categories.append(",");
                uvCtValues.append(",");
            } else {
                categories.append("]");
                uvCtValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"独立访客数\",\n" +
                "        \"data\": " + uvCtValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

    // 2. 会话数请求拦截方法
    @RequestMapping("/svCt")
    public String getPvCt(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TrafficSvCt> trafficSvCtList = trafficChannelStatsService.getSvCt(date);
        if (trafficSvCtList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder svCtValues = new StringBuilder("[");

        for (int i = 0; i < trafficSvCtList.size(); i++) {
            TrafficSvCt trafficSvCt = trafficSvCtList.get(i);
            String ch = trafficSvCt.getCh();
            Integer svCt = trafficSvCt.getSvCt();

            categories.append("\"").append(ch).append("\"");
            svCtValues.append("\"").append(svCt).append("\"");

            if (i < trafficSvCtList.size() - 1) {
                categories.append(",");
                svCtValues.append(",");
            } else {
                categories.append("]");
                svCtValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"会话数\",\n" +
                "        \"data\": " + svCtValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

    // 3. 各会话浏览页面数请求拦截方法
    @RequestMapping("/pvPerSession")
    public String getPvPerSession(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TrafficPvPerSession> trafficPvPerSessionList = trafficChannelStatsService.getPvPerSession(date);
        if (trafficPvPerSessionList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder pvPerSessionValues = new StringBuilder("[");

        for (int i = 0; i < trafficPvPerSessionList.size(); i++) {
            TrafficPvPerSession trafficPvPerSession = trafficPvPerSessionList.get(i);
            String ch = trafficPvPerSession.getCh();
            Double pvPerSession = trafficPvPerSession.getPvPerSession();

            categories.append("\"").append(ch).append("\"");
            pvPerSessionValues.append("\"").append(pvPerSession).append("\"");

            if (i < trafficPvPerSessionList.size() - 1) {
                categories.append(",");
                pvPerSessionValues.append(",");
            } else {
                categories.append("]");
                pvPerSessionValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"会话平均页面浏览数\",\n" +
                "        \"data\": " + pvPerSessionValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

    // 4. 各会话累计访问时长请求拦截方法
    @RequestMapping("/durPerSession")
    public String getDurPerSession(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TrafficDurPerSession> trafficDurPerSessionList = trafficChannelStatsService.getDurPerSession(date);
        if (trafficDurPerSessionList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder durPerSessionValues = new StringBuilder("[");

        for (int i = 0; i < trafficDurPerSessionList.size(); i++) {
            TrafficDurPerSession trafficDurPerSession = trafficDurPerSessionList.get(i);
            String ch = trafficDurPerSession.getCh();
            Double durPerSession = trafficDurPerSession.getDurPerSession();

            categories.append("\"").append(ch).append("\"");
            durPerSessionValues.append("\"").append(durPerSession).append("\"");

            if (i < trafficDurPerSessionList.size() - 1) {
                categories.append(",");
                durPerSessionValues.append(",");
            } else {
                categories.append("]");
                durPerSessionValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"会话平均页面访问时长\",\n" +
                "        \"data\": " + durPerSessionValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

    // 5. 跳出率请求拦截方法
    @RequestMapping("/ujRate")
    public String getUjRate(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TrafficUjRate> trafficUjRateList = trafficChannelStatsService.getUjRate(date);
        if (trafficUjRateList == null) {
            return "";
        }
        StringBuilder categories = new StringBuilder("[");
        StringBuilder ujRateValues = new StringBuilder("[");

        for (int i = 0; i < trafficUjRateList.size(); i++) {
            TrafficUjRate trafficUjRate = trafficUjRateList.get(i);
            String ch = trafficUjRate.getCh();
            Double ujRate = trafficUjRate.getUjRate();

            categories.append("\"").append(ch).append("\"");
            ujRateValues.append("\"").append(ujRate).append("\"");

            if (i < trafficUjRateList.size() - 1) {
                categories.append(",");
                ujRateValues.append(",");
            } else {
                categories.append("]");
                ujRateValues.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\":" + categories + ",\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"跳出率\",\n" +
                "        \"data\": " + ujRateValues + "\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

    // 6. 访客状态分时统计请求拦截方法
    @RequestMapping("/visitorPerHr")
    public String getVisitorPerHr(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TrafficVisitorStatsPerHour> visitorPerHrStatsList = trafficVisitorStatsService.getVisitorPerHrStats(date);
        if (visitorPerHrStatsList == null || visitorPerHrStatsList.size() == 0) {
            return "";
        }

        TrafficVisitorStatsPerHour[] perHrArr = new TrafficVisitorStatsPerHour[24];
        for (TrafficVisitorStatsPerHour trafficVisitorStatsPerHour : visitorPerHrStatsList) {
            Integer hr = trafficVisitorStatsPerHour.getHr();
            perHrArr[hr] = trafficVisitorStatsPerHour;
        }

        String[] hrs = new String[24];
        Long[] uvArr = new Long[24];
        Long[] pvArr = new Long[24];
        Long[] newUvArr = new Long[24];

        for (int hr = 0; hr < 24; hr++) {
            hrs[hr] = String.format("%02d", hr);
            TrafficVisitorStatsPerHour trafficVisitorStatsPerHour = perHrArr[hr];
            if (trafficVisitorStatsPerHour != null) {
                uvArr[hr] = trafficVisitorStatsPerHour.getUvCt();
                pvArr[hr] = trafficVisitorStatsPerHour.getPvCt();
                newUvArr[hr] = trafficVisitorStatsPerHour.getNewUvCt();
            } else{
                uvArr[hr] = 0L;
                pvArr[hr] = 0L;
                newUvArr[hr] = 0L;
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\": [\n\"" +
                StringUtils.join(hrs, "\",\"") + "\"\n" +
                "    ],\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"独立访客数\",\n" +
                "        \"data\": [\n" +
                StringUtils.join(uvArr, ",") + "\n" +
                "        ]\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"页面浏览数\",\n" +
                "        \"data\": [\n" +
                StringUtils.join(pvArr, ",") + "\n" +
                "        ]\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"新访客数\",\n" +
                "        \"data\": [\n" +
                StringUtils.join(newUvArr, ",") + "\n" +
                "        ]\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

    // 7. 新老访客统计请求拦截方法
    @RequestMapping("/visitorPerType")
    public String getVisitorPerType(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {

        if (date == 1) {
            date = DateUtil.now();
        }

        List<TrafficVisitorTypeStats> visitorTypeStatsList = trafficVisitorStatsService.getVisitorTypeStats(date);

        if (visitorTypeStatsList == null || visitorTypeStatsList.size() == 0) {
            return "";
        }

        // 方法二，直接拼接字符串，简单明了
        TrafficVisitorTypeStats newVisitorStats = null;
        TrafficVisitorTypeStats oldVisitorStats = null;
        for (TrafficVisitorTypeStats visitorStats : visitorTypeStatsList) {
            if ("1".equals(visitorStats.getIsNew())) {
                // 新访客
                newVisitorStats = visitorStats;
            } else {
                // 老访客
                oldVisitorStats = visitorStats;
            }
        }
        //拼接json字符串
        String json = "{\"status\":0,\"data\":{\"total\":5," +
                "\"columns\":[" +
                "{\"name\":\"类别\",\"id\":\"type\"}," +
                "{\"name\":\"新访客\",\"id\":\"new\"}," +
                "{\"name\":\"老访客\",\"id\":\"old\"}]," +
                "\"rows\":[" +
                "{\"type\":\"访客数(人)\",\"new\":" + newVisitorStats.getUvCt() + ",\"old\":" + oldVisitorStats.getUvCt() + "}," +
                "{\"type\":\"总访问页面数(次)\",\"new\":" + newVisitorStats.getPvCt() + ",\"old\":" + oldVisitorStats.getPvCt() + "}," +
                "{\"type\":\"跳出率(%)\",\"new\":" + newVisitorStats.getUjRate() + ",\"old\":" + oldVisitorStats.getUjRate() + "}," +
                "{\"type\":\"平均在线时长(秒)\",\"new\":" + newVisitorStats.getAvgDurSum() + ",\"old\":" + oldVisitorStats.getAvgDurSum() + "}," +
                "{\"type\":\"平均访问页面数(人次)\",\"new\":" + newVisitorStats.getAvgPvCt() + ",\"old\":" + oldVisitorStats.getAvgPvCt() + "}]}}";

        return json;
    }

    // 8. 关键词统计请求拦截方法
    @RequestMapping("/keywords")
    public String getKeywords(
            @RequestParam(value = "date", defaultValue = "1") Integer date) {
        if (date == 1) {
            date = DateUtil.now();
        }
        List<TrafficKeywords> keywordsList = trafficKeywordsService.getKeywords(date);
        if (keywordsList == null) {
            return "";
        }

        StringBuilder data = new StringBuilder("[");

        for (int i = 0; i < keywordsList.size(); i++) {
            TrafficKeywords trafficKeywords = keywordsList.get(i);
            String keyword = trafficKeywords.getKeyword();
            Integer keywordScore = trafficKeywords.getKeywordScore();
            data.append("" +
                    "{\n" +
                    "      \"name\": \"" + keyword + "\",\n" +
                    "      \"value\": " + keywordScore + "\n" +
                    "    }");
            if (i < keywordsList.size() - 1) {
                data.append(",");
            } else {
                data.append("]");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": " + data + "\n" +
                "}";
    }

}