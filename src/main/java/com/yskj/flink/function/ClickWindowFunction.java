package com.yskj.flink.function;

import com.yskj.flink.entity.ClickCountEvent;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/24 14:10
 */
public class ClickWindowFunction implements WindowFunction<Long, ClickCountEvent, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Long> input, Collector<ClickCountEvent> out) throws Exception {
        out.collect(new ClickCountEvent(timeWindow.getEnd(), key, input.iterator().next()));
    }
}
