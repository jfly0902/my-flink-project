package com.yskj.flink.function;

import com.google.gson.Gson;
import com.yskj.flink.entity.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/21 13:34
 */
public class UserBehaviorMapFunction implements MapFunction<String, UserBehavior> {
    @Override
    public UserBehavior map(String userBehaviorJson) throws Exception {
        System.out.println("接受到的数据： " + userBehaviorJson);
        return new Gson().fromJson(userBehaviorJson, UserBehavior.class);
    }
}
