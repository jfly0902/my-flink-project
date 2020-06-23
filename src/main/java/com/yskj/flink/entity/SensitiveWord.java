package com.yskj.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/8 14:24
 */
@AllArgsConstructor
@Data
public class SensitiveWord {

    public SensitiveWord() {}

    private String id;

    private String word;

    /**
     * 该敏感词是否可用
     * true 可用
     * false 不可用
     */
    private boolean status;
}
