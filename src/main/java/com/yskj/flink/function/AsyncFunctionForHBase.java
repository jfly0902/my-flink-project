package com.yskj.flink.function;

import cn.hutool.crypto.digest.MD5;
import com.yskj.flink.entity.DwdMsg;
import com.yskj.flink.util.HBaseClient;
import com.yskj.flink.util.HBaseContent;
import com.yskj.flink.util.MsgColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

/**
 * @Author: xiang.jin
 * @Date: 2020/6/17 13:42
 */
@Slf4j
public class AsyncFunctionForHBase extends RichAsyncFunction<DwdMsg, DwdMsg> {

    private Table table = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("开始连接hbase数据库");
        super.open(parameters);
        Connection connection = HBaseClient.getHBaseConnection();
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(HBaseContent.DWD_ROOM_TABLE_NAME);
        if (!admin.tableExists(tableName)) {
            log.info("==============不存在表 = {}", tableName);
            admin.createTable(new HTableDescriptor(TableName.valueOf(HBaseContent.DWD_ROOM_TABLE_NAME))
                    .addFamily(new HColumnDescriptor(HBaseContent.DWD_ROOM_TABLE_FAMILY)));
        }

        table = connection.getTable(tableName);
        log.info("AsyncFunctionForHBase table is connect:{}", table.getName());
    }

    @Override
    public void asyncInvoke(DwdMsg dwdMsg, ResultFuture<DwdMsg> out) throws Exception {
        /**
         * 将某个 room_id 下 的 同一天的数据放入到同一个 region中
         */
        String rowKey = getRowKey(dwdMsg.getTalker(), HBaseContent.DWD_ROOM_TABLE_REGION);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_WX_ID), Bytes.toBytes(dwdMsg.getWxId()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_TALKER), Bytes.toBytes(dwdMsg.getTalker()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_CONTENT), Bytes.toBytes(dwdMsg.getContent()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_TYPE), Bytes.toBytes(dwdMsg.getType()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_STATUS), Bytes.toBytes(dwdMsg.getStatus()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_IS_SEND), Bytes.toBytes(dwdMsg.getIsSend()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_CHAT_CREATE_TIME), Bytes.toBytes(dwdMsg.getChatCreateTime()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_ROOM_MSG_WX_ID), Bytes.toBytes(dwdMsg.getRoomMsgWxId().substring(0, dwdMsg.getRoomMsgWxId().indexOf("@"))));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_MSG_ID), Bytes.toBytes(dwdMsg.getMsgId()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_MSG_SVR_ID), Bytes.toBytes(dwdMsg.getMsgSvrId()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_TALKER_ID), Bytes.toBytes(dwdMsg.getTalkerId()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_MSG_SEQ), Bytes.toBytes(dwdMsg.getMsgSeq()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_CREATE_TIME), Bytes.toBytes(dwdMsg.getCreateTime()));
        put.addColumn(Bytes.toBytes(HBaseContent.DWD_ROOM_TABLE_FAMILY), Bytes.toBytes(MsgColumn.DWD_UPDATE_TIME), Bytes.toBytes(dwdMsg.getUpdateTime()));
        table.put(put);
        log.info("AsyncFunctionForHBase function asyncInvoke table:{} success", table.getName());
        ArrayList<DwdMsg> dwdMsgs = new ArrayList<>();
        dwdMsgs.add(dwdMsg);
        out.complete(dwdMsgs);
    }

    @Override
    public void timeout(DwdMsg input, ResultFuture<DwdMsg> out) throws Exception {
        log.info(" async function for HBase is timeout, input:{}",input);
        ArrayList<DwdMsg> list = new ArrayList<>();
        list.add(input);
        out.complete(list);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (table != null) {
            log.info("AsyncFunctionForHBase table is close:{}", table.getName());
            table.close();
        }
    }

    /**
     * 获取 RowKey
     * @param num HBase 分区数 例如： 001| 002| 003| .... 009| 等 分区数 < 10  > 10 false
     * @return str
     */
    public static String getRowKey(String talker, int num) {
        StringBuilder stringBuilder = new StringBuilder(talker);
        String dateString = getDateString();
        byte[] digest = MD5.create().digest(stringBuilder.append(dateString).toString(), "UTF-8");
        String before = getBefore(digest, num);
        String after = getAfter(digest, num);
        StringBuilder builder = new StringBuilder(before);
        long currentTimeMillis = System.currentTimeMillis();
        return builder.append(digest).append("_").append(currentTimeMillis).append("_").append(after).toString();
    }


    /**
     * 获取分区前缀
     */
    private static String getBefore(byte[] digest, int num) {
        int i = Math.abs(Arrays.hashCode(digest) % num);
        if (i < 10) {
            return  "00" + i + "_";
        } else {
            return "0" + i + "_";
        }
    }

    /**
     * 获取分区后缀
     */
    private static String getAfter(byte[] digest, int num) {
        int i = Math.abs(Arrays.hashCode(digest) % num);
        if (i < 10) {
            return  "0" + i ;
        } else {
            return i + "";
        }
    }

    /**
     * 获取时间
     */
    private static String getDateString() {
        SimpleDateFormat sim = new SimpleDateFormat("yyyyMMdd");
        return sim.format(new Date());
    }
}
