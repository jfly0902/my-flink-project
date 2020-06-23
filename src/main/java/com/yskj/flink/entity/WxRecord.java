package com.yskj.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/30 18:54
 */
@AllArgsConstructor
@Data
public class WxRecord {

    public WxRecord() {}

    /**
     * 微信号
     */
    private String wxId;

    /**
     * 和谁聊的
     */
    private String talker;

    /**
     * 群消息发送者微信号
     */
    private String roomMsgWxId;

    /** 聊天的记录数 */
    private long count;

}
