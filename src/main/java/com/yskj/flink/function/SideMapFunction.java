package com.yskj.flink.function;

import com.yskj.flink.util.HBaseClient;
import com.yskj.flink.util.HBaseTableColumn;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Map;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/7 10:50
 */
public class SideMapFunction implements MapFunction<Map<String, String>, String> {
    @Override
    public String map(Map<String, String> map) throws Exception {
        HBaseClient.writeRecord(HBaseTableColumn.ODS_WXCHAT_TABLE, " ", HBaseTableColumn.ODS_WXCHAT_FAMILY, map);
        return " ";
    }
}
