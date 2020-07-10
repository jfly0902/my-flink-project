package com.yskj.flink.function;


import com.yskj.flink.entity.DwdMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Author: xiang.jin
 * @Date: 2020/6/17 14:44
 */
@Slf4j
public class RoomMsgAggCountFunction implements AggregateFunction<DwdMsg, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(DwdMsg dwdMsg, Long aLong) {
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
