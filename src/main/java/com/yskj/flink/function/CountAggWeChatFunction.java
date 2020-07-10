package com.yskj.flink.function;

import com.yskj.flink.entity.WxChat;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Author: xiang.jin
 * @Date: 2020/7/1 17:12
 */
public class CountAggWeChatFunction implements AggregateFunction<WxChat, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(WxChat wxChat, Long aLong) {
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
