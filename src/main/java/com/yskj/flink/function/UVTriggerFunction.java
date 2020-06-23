package com.yskj.flink.function;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.Serializable;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/24 17:15
 */
/** 自定义窗口触发器 */
public class UVTriggerFunction extends Trigger<String, TimeWindow> {


    /**
     * 第一个参数 输入的数据
     * 第二个是 时间 timestamp
     */
    @Override
    public TriggerResult onElement(String s, long timestamp, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        // 每来一条数据，就处理一条然后清空内存的数据

        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

    }

}


class BloomFilter implements Serializable {

    private static final long serialVersionUID = 1L;

    // 定义位图的大小，也就是存数据的大小，16M

    private long size;

    public BloomFilter(long size) {
        this.size = size >0 ? size : 1 << 27;
    }

    // 自定义一个hash值  seed 随机数的种子

    public long bloomHash(String key, int seed) {
        long result = 0L;

        for (int i = 0; i < key.length(); i++) {
            result = result * seed + key.charAt(i);
        }
        return result & (size - 1);
    }
}