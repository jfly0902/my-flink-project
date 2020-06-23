package com.yskj.flink.core;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/28 17:19
 */
/** AssignerWithPeriodicWatermarks<String> 参数是：此分配程序为其分配时间戳的元素的类型 */
public class CustomTimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<String> {

    /**
     * 通过 getCurrentWatermark 来探测下一个 waterMark的值
     * 如果探测值为非空且时间戳大于上一个waterMark的时间戳（以保持升序水印的契约），系统将生成新的水印。
     *
     * 该方法被系统周期性地调用以检索当前 waterMark
     *
     * @return
     */
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return null;
    }

    /**
     * 该方法被传递给先前分配的元素时间戳。element
     * 上一个时间戳可能是由上一个分配者通过接收时间分配的。
     * 如果元素之前没有带时间戳，则该值为{@code Long.MIN_value}。
     *
     * @param element 将为其分配时间戳的元素
     * @param previousElementTimestamp 1
     * @return 返回一个新的 Timestamp
     */
    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        return 0;
    }
}
