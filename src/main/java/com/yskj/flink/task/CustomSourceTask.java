package com.yskj.flink.task;

import com.yskj.flink.core.CustomTimestampsAndWatermarks;
import com.yskj.flink.function.CountAgg;
import com.yskj.flink.function.WindowResultFunction;
import com.yskj.flink.source.CustomSourceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/28 15:50
 */
public class CustomSourceTask {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         * timeWindowAll 实际上就是
         * 底层就是就是调用的
         * windowAll(TumblingProcessingTimeWindows.of(size));
         * 根据 timeWindowAll()传入的参数不一样获取不同的WindowAll
         *  timeWindowAll/windowAll是一个数据流，不会根据key进行分组
         *
         *
         */
        env.addSource(new CustomSourceFunction()).assignTimestampsAndWatermarks(new CustomTimestampsAndWatermarks())
                .keyBy(0)
                /**
                 * timeWindow(Time.hours(1))
                 * 时间底层就是调用的
                 * window()
                 * window(SlidingProcessingTimeWindows.of(size, slide));
                 * 根据timeWindow()传参的不同获取不同的window
                 * timeWindow/window 返回都是 WindowedStream
                 * 是根据key分组 windowStream
                 */
                .timeWindow(Time.hours(1));
                /** aggregate 返回值
                 * 结果流中元素的类型，等于AggregateFunction的结果类型
                 */
                //.aggregate(new CountAgg(), new WindowResultFunction())
    }

}
