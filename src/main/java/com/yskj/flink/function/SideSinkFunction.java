package com.yskj.flink.function;

import cn.hutool.crypto.digest.MD5;
import com.yskj.flink.util.HBaseClientUtils;
import com.yskj.flink.util.HBaseTableColumn;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/11 11:00
 */
public class SideSinkFunction implements SinkFunction<Map<String, String>> {

    @Override
    public void invoke(Map<String, String> value, Context context) throws Exception {
        String rowKey = getRowKey(value);
        HBaseClientUtils.writeRecord(HBaseTableColumn.SIDE_WXCHAT_TABLE, rowKey, HBaseTableColumn.SIDE_WXCHAT_FAMILY, value);
    }

    private String getRowKey(Map<String, String> value) {
        StringBuilder rowKey = new StringBuilder();
        byte[] digest = MD5.create().digest(value.get("wxId") + value.get("talker"));
        int i = Arrays.hashCode(digest) % 3;
        int abs = Math.abs(i);
        String before = "00" +abs + "_";
        String dateString = getDateString();
        return rowKey.append(before).append(dateString).append("_").append(digest).toString();
    }

    private String getDateString() {
        SimpleDateFormat sim = new SimpleDateFormat("yyyyMMdd");
        return sim.format(new Date());
    }
}
