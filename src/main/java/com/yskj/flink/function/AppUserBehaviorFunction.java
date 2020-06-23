package com.yskj.flink.function;

import com.yskj.flink.entity.AppUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/23 18:43
 */
public class AppUserBehaviorFunction implements MapFunction<String, AppUserBehavior> {
    @Override
    public AppUserBehavior map(String s) throws Exception {
        String[] split = s.split(",");
        AppUserBehavior appUserBehavior = new AppUserBehavior();
        appUserBehavior.setUserId(split[0]);
        appUserBehavior.setBehavior(split[1]);
        appUserBehavior.setChannel(split[2]);
        appUserBehavior.setTimestamp(split[2]);
        return appUserBehavior;
    }
}
