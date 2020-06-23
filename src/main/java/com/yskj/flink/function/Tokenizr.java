package com.yskj.flink.function;

import com.yskj.flink.entity.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/22 18:03
 */
public class Tokenizr implements FlatMapFunction<UserBehavior, String> {
    @Override
    public void flatMap(UserBehavior userBehavior, Collector<String> collector) throws Exception {
        collector.collect("");
    }
}
