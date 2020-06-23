package com.yskj.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/24 14:19
 */
@Data
@AllArgsConstructor
public class ClickCountEvent {

    private long windowEnd;

    private String key;

    private long count;
}
