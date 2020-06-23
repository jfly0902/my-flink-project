package com.yskj.flink.function;

import com.yskj.flink.util.HBaseClient;
import com.yskj.flink.util.HBaseTableColumn;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Map;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/7 10:34
 */
public class SideOutPutFunction extends RichSinkFunction<Map<String, String>> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        HBaseClient.open(HBaseTableColumn.SIDE_WXCHAT_TABLE, HBaseTableColumn.SIDE_WXCHAT_FAMILY);
    }

    @Override
    public void close() throws Exception {
        super.close();
        HBaseClient.close();
    }

    @Override
    public void invoke(Map<String, String> value, Context context) throws Exception {
        HBaseClient.writeRecord(HBaseTableColumn.SIDE_WXCHAT_TABLE, " ",HBaseTableColumn.SIDE_WXCHAT_FAMILY, value);
    }

}
