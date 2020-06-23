package com.yskj.flink.function;

import com.yskj.flink.entity.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/21 13:49
 */
public class CountFunction implements AggregateFunction<UserBehavior, Long, Long> {
    //初始化值
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    // 计算规则，来一条数据即 +1
    @Override
    public Long add(UserBehavior userBehavior, Long aLong) {
        return aLong + 1;
    }

    // 返回计算的结果
    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    // 合并数据
    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}
