package com.yskj.flink.function;

import com.yskj.flink.entity.AdClickEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/24 14:07
 */
public class ClickCount implements AggregateFunction<AdClickEvent, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(AdClickEvent adClickEvent, Long aLong) {
        return adClickEvent.getClk_cnt() + aLong;
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
