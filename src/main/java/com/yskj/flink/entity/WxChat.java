package com.yskj.flink.entity;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;


/**
 * @Author: xiang.jin
 * @Date: 2020/4/30 18:24
 */
public class WxChat extends Tuple5<String, String, String, String, String> {

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
     * 时间戳
     */
    private String chatCreateTime;

    /**
     * 聊天内容消息类型,  587202609 分享内容 570425393 群邀请 469762097 收到红包 436207665 发红包  419430449 收到转账微信转账 268435505 分享连接(外部连接) 16777265 分享连接 外部连接 1048625 分享 无效内容  10000添加好友成功提示(系统提示) 10002 系统提示 邀请好友数量  66 未知  64 语音提示 62 未知 50微信语音聊天 49 文件(历史数据有错误) 48定位 47表情 43视频 42名片 34语音  3图片  1文本 -1879048186 位置共享
     */
    private String type;

    public WxChat() {}

    public WxChat(String wxId, String talker, String roomMsgWxId, String chatCreateTime, String type) {
        this.f0 = wxId;
        this.f1 = talker;
        this.f2 = roomMsgWxId;
        this.f3 = chatCreateTime;
        this.f4 = type;
    }

    public String getWxId() {
        return this.f0;
    }

    public void setWxId(String wxId) {
        this.f0 = wxId;
    }

    public String getTalker() {
        return this.f1;
    }

    public void setTalker(String talker) {
        this.f1 = talker;
    }

    public String getRoomMsgWxId() {
        return this.f2;
    }

    public void setRoomMsgWxId(String roomMsgWxId) {
        this.f2 = roomMsgWxId;
    }

    public String getChatCreateTime() {
        return this.f3;
    }

    public void setChatCreateTime(String chatCreateTime) {
        this.f3 = chatCreateTime;
    }

    public String getType() {
        return this.f4;
    }

    public void setType(String type) {
        this.f4 = type;
    }
}
