package com.yskj.flink.entity;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/21 13:53
 */
public class ItemViewCount {
    public long itemId;     // 商品ID
    public long windowEnd;  // 窗口结束时间戳
    public long viewCount;  // 商品的点击量

    public static ItemViewCount of (long itemId, long windowEnd, long viewCount) {
        ItemViewCount itemViewCount = new ItemViewCount();
        itemViewCount.itemId = itemId;
        itemViewCount.windowEnd = windowEnd;
        itemViewCount.viewCount = viewCount;
        return itemViewCount;
    }
}
