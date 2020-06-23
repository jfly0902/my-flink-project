package com.yskj.flink.function;

import com.google.gson.Gson;
import com.yskj.flink.entity.RoomMember;
import com.yskj.flink.util.HBaseClient;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/28 10:39
 */
public class RoomMapFunction implements MapFunction<String, RoomMember> {
    @Override
    public RoomMember map(String s) throws Exception {
        RoomMember roomMember = new Gson().fromJson(s, RoomMember.class);
        // 将数据存入hbase中
        try{
            //HBaseClient.writeRecord("sdsdf","s","s",RoomMember.class);
        } catch (Exception e) {

        }
        return roomMember;
    }
}
