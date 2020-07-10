package com.yskj.flink.function;

import com.alibaba.fastjson.JSON;
import com.yskj.flink.entity.DwdMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;

import java.text.SimpleDateFormat;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @Author: xiang.jin
 * @Date: 2020/6/17 13:36
 */
@Slf4j
public class RoomMsgMapFunction implements MapFunction<String, DwdMsg> {
    @Override
    public DwdMsg map(String jsonString) throws Exception {
        log.info("====> 进来的数据:{}", jsonString);
        DwdMsg dwdMsg = JSON.parseObject(jsonString, DwdMsg.class);
        dwdMsg.setWxId(dwdMsg.getWxId() + "@" + ThreadLocalRandom.current().nextInt(20));
        if (null == dwdMsg.getRoomMsgWxId()) {
            dwdMsg.setRoomMsgWxId("" + "@" + ThreadLocalRandom.current().nextInt(20));
        } else {
            dwdMsg.setRoomMsgWxId(dwdMsg.getRoomMsgWxId() + "@" + ThreadLocalRandom.current().nextInt(20));
        }
        if (null == dwdMsg.getContent()) {
            dwdMsg.setContent("");
        }
        if (null == dwdMsg.getMsgId()) {
            dwdMsg.setMsgId("");
        }
        if (null == dwdMsg.getTalkerId()) {
            dwdMsg.setTalkerId("");
        }
        if (null == dwdMsg.getMsgSeq()) {
            dwdMsg.setMsgSeq("");
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String chatDate = sdf.format(Long.parseLong(dwdMsg.getChatCreateTime()));
        dwdMsg.setChatDate(chatDate);
        return dwdMsg;
    }
}
