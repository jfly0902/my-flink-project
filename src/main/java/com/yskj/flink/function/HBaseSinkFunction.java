package com.yskj.flink.function;

import cn.hutool.crypto.digest.MD5;
import com.yskj.flink.util.HBaseClientUtils;
import com.yskj.flink.util.HBaseTableColumn;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/7 10:14
 */
public class HBaseSinkFunction extends RichSinkFunction<Map<String, String>> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        HBaseClientUtils.open(HBaseTableColumn.DW_WXCHAT_TABLE, HBaseTableColumn.DW_WXCHAT_FAMILY);
    }

    @Override
    public void close() throws Exception {
        super.close();
        HBaseClientUtils.close();
    }

    @Override
    public void invoke(Map<String, String> value, Context context) throws Exception {
        String rowKey = getRowKey(value);
        HashMap<String, String> map = new HashMap<>();
        for (String key :
                value.keySet()) {
            map.put(key, value.get(key));
        }
        HBaseClientUtils.writeRecord(HBaseTableColumn.DW_WXCHAT_TABLE, rowKey, HBaseTableColumn.DW_WXCHAT_FAMILY, map);
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
