package com.yskj.flink.function;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/14 10:45
 */
public class ResultWxChatFunction implements WindowFunction<Long, Map<String, String>, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Long> input, Collector<Map<String, String>> out) throws Exception {
        long windowEnd = timeWindow.getEnd();
        long windowStart = timeWindow.getStart();
        System.out.println(" =====> ResultWxChatFunction window start:" + windowStart);
        System.out.println(" =====> ResultWxChatFunction window End:" + windowEnd);
        Iterator<Long> iterator = input.iterator();
        Long count = iterator.next();
        Map<String, String> map = new HashMap<>();
        String[] split = key.split(",");
        map.put("wxId", split[0]);
        map.put("talker", split[1]);
        map.put("roomMsgWxId", split[2]);
        map.put("count", count.toString());
        out.collect(map);
    }
}
