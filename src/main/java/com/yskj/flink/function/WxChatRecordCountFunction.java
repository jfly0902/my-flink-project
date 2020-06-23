package com.yskj.flink.function;

import com.yskj.flink.entity.WxChat;
import com.yskj.flink.entity.WxRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.Map;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/30 18:49
 */
public class WxChatRecordCountFunction implements AggregateFunction<Map<String, String>, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Map<String, String> map, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}
