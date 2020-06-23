package com.yskj.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/8 14:20
 */
@AllArgsConstructor
@Data
public class SensitiveWxChat {

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

    /**
     * 时间戳，消息创建时间
     */
    private Long chatCreateTime;

    /**
     * 聊天内容
     */
    private String content;

}
