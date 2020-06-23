package com.yskj.flink.util;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/28 13:48
 */
public class HBaseTableColumn {
    /**
     * wxChat的明细数据，将json数据elk成明细HBase的结构化数据
     */
    public static final String ODS_WXCHAT_TABLE = "ods_wxChat";
    public static final String ODS_WXCHAT_FAMILY = "of";

    /**
     * 经过计算统计每个用户的每天发言次数
     */
    public static final String DW_WXCHAT_TABLE = "dw_wxChat";
    public static final String DW_WXCHAT_FAMILY = "df";

    /**
     * 延迟的数据
     */
    public static final String SIDE_WXCHAT_TABLE = "side_wxChat";
    public static final String SIDE_WXCHAT_FAMILY = "sf";


    /**
     * 敏感用户数据明细
     */
    public static final String SEN_WXCHAT_TABLE = "sensitive_wxChat";
    public static final String SEN_WXCHAT_FAMILY = "sf";


}
