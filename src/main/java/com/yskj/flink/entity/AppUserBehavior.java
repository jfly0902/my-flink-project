package com.yskj.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/23 17:34
 */
@Data
@AllArgsConstructor
public class AppUserBehavior {

    private String userId;
    private String behavior;
    private String channel;
    private String timestamp;

    public AppUserBehavior() {}

}
