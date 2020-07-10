package com.yskj.flink.util;

/**
 * @Author: xiang.jin
 * @Date: 2020/6/17 9:44
 */
public class HBaseContent {

    /** HBase 明细层 dwd */
    public static final String DWD_MEMBER_TABLE_NAME = "dwd-member-msg";
    public static final String DWD_MEMBER_TABLE_FAMILY = "mf";
    public static final int DWD_MEMBER_TABLE_REGION = 10;

    public static final String DWD_ROOM_TABLE_NAME = "dwd-room-msg";
    public static final String DWD_ROOM_TABLE_FAMILY = "rf";
    public static final int DWD_ROOM_TABLE_REGION =1;

    /** HBase 轻度汇总层 dws */
    public static final String DWS_MEMBER_TABLE_NAME = "dws-member-msg";
    public static final String DWS_MEMBER_TABLE_FAMILY = "mf";
    public static final int DWS_MEMBER_TABLE_REGION = 2;

    public static final String DWS_ROOM_TABLE_NAME = "dws-room-msg";
    public static final String DWS_ROOM_TABLE_FAMILY = "rf";
    public static final int DWS_ROOM_TABLE_REGION = 1;

    /** HBase 延迟数据 dwd */
    public static final String DWD_ROOM_OUTPUT_TABLE_NAME = "dwd-room-outPut-msg";
    public static final String DWD_ROOM_OUTPUT_TABLE_FAMILY = "rf";
    public static final int DWD_ROOM_OUTPUT_TABLE_REGION =1;
}
