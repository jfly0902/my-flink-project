package com.yskj.flink.function;

import cn.hutool.crypto.digest.MD5;
import com.alibaba.fastjson.JSON;
import com.yskj.flink.util.HBaseClientUtils;
import com.yskj.flink.util.HBaseTableColumn;
import org.apache.flink.api.common.functions.MapFunction;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/11 9:41
 */
public class KafkaJson2HBaseFunction implements MapFunction<String, Map<String, String>> {

    @Override
    public Map<String, String> map(String input) throws Exception {
        System.out.println("===================> KafkaJson2HBaseFunction :" + input);
        // 将json直接拉平成map对象
        Map<String, String> jsonMap = (Map)JSON.parse(input);

        // 获得rowKey
        String rowKey = getRowKey(jsonMap);

        HBaseClientUtils.writeRecord(HBaseTableColumn.ODS_WXCHAT_TABLE, rowKey, HBaseTableColumn.ODS_WXCHAT_FAMILY, jsonMap);

        Map<String, String> newMap = new ConcurrentHashMap<>();
        newMap.put("wxId", jsonMap.get("wxId"));
        newMap.put("talker", jsonMap.get("talker"));
        newMap.put("roomMsgWxId", jsonMap.get("roomMsgWxId"));
        newMap.put("chatCreateTime", jsonMap.get("chatCreateTime"));
        return newMap;
    }

    private String getRowKey(Map<String, String> jsonMap) {
        // 预分区21个
        StringBuilder rowKey = new StringBuilder();
        byte[] digest = MD5.create().digest(jsonMap.get("wxid") + jsonMap.get("talker"));
        String before;
        int i = Arrays.hashCode(digest) % 21;
        int abs = Math.abs(i);
        if (abs < 10) {
            before = "00" + abs + "_";
        }
        before = "0" +abs + "_";
        String dateString = getDateString();
        return rowKey.append(before).append(dateString).append("_").append(digest).toString();
    }

    private String getDateString() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        return simpleDateFormat.format(new Date());
    }


}
