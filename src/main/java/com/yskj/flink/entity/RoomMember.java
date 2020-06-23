package com.yskj.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/28 10:46
 */
@Data
@AllArgsConstructor
@TypeInfo(TypeInfoFactory.class)
public class RoomMember {
    public String roomId;
    public String wxid;
    public String addTime;
    public String createTime;
    public String updateTime;
}
