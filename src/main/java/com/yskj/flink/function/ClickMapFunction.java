package com.yskj.flink.function;

import com.yskj.flink.entity.AdClickEvent;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/24 13:41
 */
public class ClickMapFunction implements MapFunction<String, AdClickEvent> {
    @Override
    public AdClickEvent map(String s) throws Exception {
        String[] split = s.split(",");
        return new AdClickEvent(split[0], split[1], split[2], split[3], split[4], Integer.valueOf(split[5]), split[6]);
    }
}
