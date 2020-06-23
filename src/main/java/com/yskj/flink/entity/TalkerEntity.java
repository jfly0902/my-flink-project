package com.yskj.flink.entity;

/**
 * @Author: jinxiang
 * @Date: 2020/4/20 11:23
 */
public class TalkerEntity {

    private String talker;

    private String content;

    private String wxId;

    private String chatCreateTime;

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

    public String getWxId() {
        return wxId;
    }

    public void setWxId(String wxId) {
        this.wxId = wxId;
    }

    public String getChatCreateTime() {
        return chatCreateTime;
    }

    public void setChatCreateTime(String chatCreateTime) {
        this.chatCreateTime = chatCreateTime;
    }

    @Override
    public String toString() {
        return "TalkerEntity{" +
                "talker='" + talker + '\'' +
                ", content='" + content + '\'' +
                ", wxId='" + wxId + '\'' +
                ", chatCreateTime='" + chatCreateTime + '\'' +
                '}';
    }
}
