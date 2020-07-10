package com.yskj.flink.sink;

import cn.hutool.crypto.digest.MD5;
import com.yskj.flink.entity.DwsMsg;
import com.yskj.flink.util.HBaseClient;
import com.yskj.flink.util.HBaseContent;
import com.yskj.flink.util.MsgColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: xiang.jin
 * @Date: 2020/6/17 15:42
 */
@Slf4j
public class DwsMsgSinkForHBase extends RichSinkFunction<DwsMsg> {

    private Table table = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Connection connection = HBaseClient.getHBaseConnection();
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf(HBaseContent.DWS_ROOM_TABLE_NAME);
        if (!admin.tableExists(tableName)) {
            log.info("==============不存在表 = {}", tableName);
            admin.createTable(new HTableDescriptor(TableName.valueOf(HBaseContent.DWS_ROOM_TABLE_NAME))
                    .addFamily(new HColumnDescriptor(HBaseContent.DWS_ROOM_TABLE_FAMILY)));
        }

        table = connection.getTable(tableName);
        log.info("DwsMsgSinkForHBase table is connect:{}", table.getName());
    }

    @Override
    public void invoke(DwsMsg dwsMsg, Context context) throws Exception {
        /**
         * 创建一个7天的数据表，一个分区足够了
         *
         * MD5(room_id ) _ (yyyyMMdd) _ MD5(roomMsgWxId 客户wxid)
         * 12 + 9 + 11 = 32
         */
        String rowKey = getRowKey(dwsMsg);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(HBaseContent.DWS_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWS_TALKER), Bytes.toBytes(dwsMsg.getTalker()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWS_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWS_WECHAT_TIME), Bytes.toBytes(dwsMsg.getWeChatTime()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWS_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWS_ROOM_MSG_WX_ID), Bytes.toBytes(dwsMsg.getRoomMsgWxId()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWS_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWS_PV_VALUE), Bytes.toBytes(dwsMsg.getPvValue()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWS_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWS_WINDOW_START), Bytes.toBytes(dwsMsg.getWindowStart()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWS_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWS_WINDOW_END), Bytes.toBytes(dwsMsg.getWindowEnd()));
        table.put(put);
        log.info("DwsMsgSinkForHBase function sink invoke table:{} success", table.getName());
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (table != null) {
            log.info("DwsMsgSinkForHBase table is close:{}", table.getName());
            table.close();
        }
    }


    /**
     * 获取 RowKey
     */
    public static String getRowKey(DwsMsg dwsMsg) {
        String dateString = getDateString();
        byte[] digest = MD5.create().digest(dwsMsg.getTalker(), "UTF-8");
        byte[] useId = MD5.create().digest(dwsMsg.getRoomMsgWxId(), "UTF-8");
        StringBuilder builder = new StringBuilder();
        return builder.append(digest).append("_").append(dateString).append("_").append(useId).toString();
    }

    /**
     * 获取时间
     */
    private static String getDateString() {
        SimpleDateFormat sim = new SimpleDateFormat("yyyyMMdd");
        return sim.format(new Date());
    }
}
