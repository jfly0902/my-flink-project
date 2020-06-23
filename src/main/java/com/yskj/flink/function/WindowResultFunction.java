package com.yskj.flink.function;

import com.yskj.flink.entity.ItemViewCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/21 13:53
 */
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

    // 窗口的主键，即 itemId
    // input 聚合函数的结果，即 count 值
    // timeWindow 窗口
    // out 返回的值
    @Override
    public void apply(Tuple key, TimeWindow timeWindow, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
        System.out.println("参数 key：" + key + " timeWindow: " + timeWindow + " input: " + input.iterator().next());
        long itemId = key.getField(0);
        long windowEnd = timeWindow.getEnd();
        Long viewCount = input.iterator().next();
        out.collect(ItemViewCount.of(itemId, windowEnd, viewCount));
    }
}
