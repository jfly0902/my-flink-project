package com.yskj.flink.function;

import cn.hutool.crypto.digest.MD5;
import com.yskj.flink.util.HBaseClientUtils;
import com.yskj.flink.util.HBaseTableColumn;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/11 10:44
 */
public class HBaseDWSinkFunction extends RichSinkFunction<Map<String, String>> {

    @Override
    public void close() throws Exception {
        super.close();
        HBaseClientUtils.close();
    }

    @Override
    public void invoke(Map<String, String> value, Context context) throws Exception {
        // 计数的数据，数据分区为3个区 000|001|002|
        HashMap<String, String> result = new HashMap<>();
        for (String key : value.keySet()) {
            String[] split = key.split(",");
            for (int i = 0; i < split.length ; i++) {
                result.put(split[i], split[i]);
            }
            result.put("count", value.get(key));
        }
        String rowKey = getRowKey(result);
        HBaseClientUtils.writeRecord(HBaseTableColumn.DW_WXCHAT_TABLE, rowKey, HBaseTableColumn.DW_WXCHAT_FAMILY, result);
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
