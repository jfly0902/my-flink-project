package com.yskj.flink.entity;

/**
 * @Author: xiang.jin
 * @Date: 2020/6/16 17:45
 */
public class DwdMsg {

    private String id;
    private String wxId;
    private String talker;
    private String content;
    private String type;
    private String status;
    private String isSend;
    private String chatCreateTime;
    private String chatDate;
    private String roomMsgWxId;
    private String msgId;
    private String msgSvrId;
    private String talkerId;
    private String msgSeq;
    private String createTime;
    private String updateTime;

    public DwdMsg() {}


    public String getChatDate() {
        return chatDate;
    }

    public void setChatDate(String chatDate) {
        this.chatDate = chatDate;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getWxId() {
        return wxId;
    }

    public void setWxId(String wxId) {
        this.wxId = wxId;
    }

    public String getTalker() {
        return talker;
    }

    public void setTalker(String talker) {
        this.talker = talker;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getIsSend() {
        return isSend;
    }

    public void setIsSend(String isSend) {
        this.isSend = isSend;
    }

    public String getChatCreateTime() {
        return chatCreateTime;
    }

    public void setChatCreateTime(String chatCreateTime) {
        this.chatCreateTime = chatCreateTime;
    }

    public String getRoomMsgWxId() {
        return roomMsgWxId;
    }

    public void setRoomMsgWxId(String roomMsgWxId) {
        this.roomMsgWxId = roomMsgWxId;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getMsgSvrId() {
        return msgSvrId;
    }

    public void setMsgSvrId(String msgSvrId) {
        this.msgSvrId = msgSvrId;
    }

    public String getTalkerId() {
        return talkerId;
    }

    public void setTalkerId(String talkerId) {
        this.talkerId = talkerId;
    }

    public String getMsgSeq() {
        return msgSeq;
    }

    public void setMsgSeq(String msgSeq) {
        this.msgSeq = msgSeq;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
}
