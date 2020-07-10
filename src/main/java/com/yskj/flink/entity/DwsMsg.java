package com.yskj.flink.entity;


/**
 * @Author: xiang.jin
 * @Date: 2020/6/17 14:54
 */
public class DwsMsg {

    private String talker;
    private String weChatTime;
    private String roomMsgWxId;
    private String pvValue;
    private String windowStart;
    private String windowEnd;

    public DwsMsg() {}

    public DwsMsg(String talker, String weChatTime, String roomMsgWxId, String pvValue, String windowStart, String windowEnd) {
        this.talker = talker;
        this.weChatTime = weChatTime;
        this.roomMsgWxId = roomMsgWxId;
        this.pvValue = pvValue;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public String getWeChatTime() {
        return weChatTime;
    }

    public void setWeChatTime(String weChatTime) {
        this.weChatTime = weChatTime;
    }

    public String getTalker() {
        return talker;
    }

    public void setTalker(String talker) {
        this.talker = talker;
    }

    public String getRoomMsgWxId() {
        return roomMsgWxId;
    }

    public void setRoomMsgWxId(String roomMsgWxId) {
        this.roomMsgWxId = roomMsgWxId;
    }

    public String getPvValue() {
        return pvValue;
    }

    public void setPvValue(String pvValue) {
        this.pvValue = pvValue;
    }

    public String getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "DwsMsg{" +
                "talker='" + talker + '\'' +
                ", weChatTime='" + weChatTime + '\'' +
                ", roomMsgWxId='" + roomMsgWxId + '\'' +
                ", pvValue='" + pvValue + '\'' +
                ", windowStart='" + windowStart + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                '}';
    }
}
