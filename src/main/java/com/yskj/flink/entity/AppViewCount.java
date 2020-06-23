package com.yskj.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/23 19:02
 */
@Data
@AllArgsConstructor
public class AppViewCount {

    private String start;
    private String end;
    private String channel;
    private String behavior;
    private int count;

}
