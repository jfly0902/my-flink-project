package com.yskj.flink.entity;


import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/30 18:14
 */
@Data
@AllArgsConstructor
public class WxChatHistoryCloud {

    private Long id;

    /**
     * 微信号
     */
    private String wxId;

    /**
     * 和谁聊的
     */
    private String talker;

    /**
     * 聊天内容消息类型,  587202609 分享内容 570425393 群邀请 469762097 收到红包 436207665 发红包  419430449 收到转账微信转账 268435505 分享连接(外部连接) 16777265 分享连接 外部连接 1048625 分享 无效内容  10000添加好友成功提示(系统提示) 10002 系统提示 邀请好友数量  66 未知  64 语音提示 62 未知 50微信语音聊天 49 文件(历史数据有错误) 48定位 47表情 43视频 42名片 34语音  3图片  1文本 -1879048186 位置共享
     */
    private Integer type;

    /**
     * 消息状态
     */
    private Integer status;

    /**
     * 是否是发送的 1、发送，0、接收
     */
    private Integer isSend;

    /**
     * 时间戳，消息创建时间
     */
    private Long chatCreateTime;

    /**
     * 群消息发送者微信号
     */
    private String roomMsgWxId;

    /**
     * 客户端消息id
     */
    private Long msgId;

    /**
     * 微信消息唯一表示id
     */
    private Long msgSvrId;
    private String talkerId;
    private String msgSeq;

    /**
     * 创建时间
     */
    private String createTime;

    /**
     * 更新时间
     */
    private String updateTime;

    /**
     * 互动次数全量状态 0：未跑过； 1：已跑过
     */
    private Boolean interactNumStatus;

    /**
     * 同行统计全量状态 0：未跑过； 1：已跑过
     */
    private Boolean peerStatus;

    /**
     * 聊天内容
     */
    private String content;

    /**
     * 图片地址
     */
    private String imgPath;
}
