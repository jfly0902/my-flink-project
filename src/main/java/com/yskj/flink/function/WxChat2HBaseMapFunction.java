package com.yskj.flink.function;

import cn.hutool.crypto.digest.MD5;
import com.alibaba.fastjson.JSON;
import com.yskj.flink.util.HBaseClientUtils;
import com.yskj.flink.util.HBaseTableColumn;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/30 18:23
 */
public class WxChat2HBaseMapFunction extends RichAsyncFunction<String, Map<String, String>> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        HBaseClientUtils.open(HBaseTableColumn.ODS_WXCHAT_TABLE, HBaseTableColumn.ODS_WXCHAT_FAMILY);
    }

    @Override
    public void close() throws Exception {
        super.close();
        HBaseClientUtils.close();
    }

    @Override
    public void asyncInvoke(String json, ResultFuture<Map<String, String>> resultFuture) throws Exception {
        Map<String, String> jsonMap = (HashMap<String, String>) JSON.parse(json);
        // 将map中的数据存入HBASE中，然后返回一个新的map
        // rowKey的设计
        /**
         * 根据每天20W的数据量来计算，一条数据1k,一天的数据大概事195M
         * 一年的数据大概70G的数据量，3年的数据也就事210G
         * 一个HFile默认的大小是10G,官网建议每个RS 20~200个regions是比较合理的
         * 因此可以将预分区的个数根据10G一个分区，预先分区的个数为21个
         * 因此与分区是000|001|...|020| 21个分区
         * 考虑到统计的时候每天和近7天的数据统计的频率较高
         * 因此使用 wxId + talker + yyyyMM % 21 得到分区的大小然后进行拼接拿到签4位数据
         * 然后使用Long.Max_value-timeStamp + （Wxid + talker）MD5拼接为后面的数据
         * 使用时间在前面主要是因为 统计天的数据次数比较多，因此根据次方法保证数据的散列和唯一性
         * 001_yyyyMMdd_MD5.create().digest((wxId + talker).getBytes());
         *
         */
        // 获取rowKey

        String rowKey = getRowKey(jsonMap);

        // 将数据写入Hbase中
        HBaseClientUtils.writeRecord(HBaseTableColumn.ODS_WXCHAT_TABLE, rowKey, HBaseTableColumn.ODS_WXCHAT_FAMILY, jsonMap);
        // 新的MAP
        Map<String, String> newMap = new HashMap<>();
        newMap.put("wxId", jsonMap.get("wxId"));
        newMap.put("talker", jsonMap.get("talker"));
        newMap.put("chatCreateTime", jsonMap.get("chatCreateTime"));
        newMap.put("roomMsgWxId", jsonMap.get("roomMsgWxId"));
        resultFuture.complete(Collections.singleton(newMap));
    }

    private String getRowKey(Map<String, String> jsonMap) {
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
        return rowKey.append(before).append(dateString).append("_").append(digest.toString()).toString();
    }

    private String getDateString() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        return simpleDateFormat.format(new Date());
    }
}
