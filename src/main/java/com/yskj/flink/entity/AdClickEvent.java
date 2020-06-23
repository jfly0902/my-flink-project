package com.yskj.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/24 9:32
 */
@Data
@AllArgsConstructor
public class AdClickEvent {

    private String userId;
    private String theme_id;
    private String item_id;
    private String leaf_cate_id;
    private String cate_level1_id;
    private int clk_cnt;
    private String reach_time;

    public AdClickEvent() {}

}
