package com.yskj.flink.function;

import com.alibaba.fastjson.JSON;
import com.yskj.flink.entity.TalkerEntity;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: jinxiang
 * @Date: 2020/4/20 10:53
 */
public class JsonMapFunction implements MapFunction<String, TalkerEntity> {

    @Override
    public TalkerEntity map(String json) throws Exception {
        return getTalkerEntity(json);
    }

    private TalkerEntity getTalkerEntity(String json) {
        System.out.println(json);
        return JSON.parseObject(json, TalkerEntity.class);
    }
}
