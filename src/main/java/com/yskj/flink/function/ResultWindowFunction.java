package com.yskj.flink.function;

import com.yskj.flink.entity.DwsMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: xiang.jin
 * @Date: 2020/6/17 14:52
 */
@Slf4j
public class ResultWindowFunction implements WindowFunction<Long, DwsMsg, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Long> input, Collector<DwsMsg> out) throws Exception {
        long windowStart = timeWindow.getStart();
        long windowEnd = timeWindow.getEnd();
        String[] split = key.split(",");
        String weChatTime = split[0];
        String talker = split[1];
        String roomMsgWxId = split[2];
        long pvValue = input.iterator().next();
        DwsMsg dwsMsg = new DwsMsg(talker, weChatTime,roomMsgWxId.substring(0, roomMsgWxId.indexOf("@")), String.valueOf(pvValue),
                String.valueOf(windowStart), String.valueOf(windowEnd));
        out.collect(dwsMsg);
    }
}
