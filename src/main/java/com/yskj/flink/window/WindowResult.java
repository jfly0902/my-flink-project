package com.yskj.flink.window;

import com.yskj.flink.entity.UserTalkerEntity;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: jinxiang
 * @Date: 2020/4/20 14:59
 */
public class WindowResult implements WindowFunction<Long, UserTalkerEntity, String, TimeWindow> {


    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Long> input, Collector<UserTalkerEntity> out) throws Exception {

        String itemId = key;

        Long count = input.iterator().next();

        out.collect(UserTalkerEntity.of(itemId, timeWindow.getEnd(), count));

    }
}
