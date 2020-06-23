package com.yskj.flink.entity;

import lombok.Data;

/**
 * @Author: jinxiang
 * @Date: 2020/4/20 15:14
 */
@Data
public class UserTalkerEntity {

    private String talker;

    private long windowEnd;

    private long count;

    public static UserTalkerEntity of (String itemId, long end, long cn) {
        UserTalkerEntity entity = new UserTalkerEntity();
        entity.setTalker(itemId);
        entity.setWindowEnd(end);
        entity.setCount(cn);
        return entity;
    }

}
