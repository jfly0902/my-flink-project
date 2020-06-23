package com.yskj.flink.util;

import com.yskj.flink.entity.WxChatHistoryCloud;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/28 10:54
 */
@Slf4j
public class HBaseClient {

    private static Admin admin;
    private static Connection connection;
    private static Configuration conf;
    private static Table table;
    private static volatile boolean flag = true;

    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", Property.getValue("hbase.rootdir"));
        conf.set("hbase.zookeeper.quorum", Property.getValue("hbase.zookeeper.quorum"));
        conf.set("hbase.client.scanner.timeout.period", Property.getValue("hbase.client.scanner.timeout.period"));
        conf.set("hbase.rpc.timeout", Property.getValue("hbase.rpc.timeout"));
        conf.set("hbase.master", Property.getValue("hbase.master"));
        conf.set("hbase.zookeeper.property.clientPort", Property.getValue("hbase.zookeeper.property.clientPort"));
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            log.info("获取admin:{}" + admin);
        } catch (IOException e) {
            log.error("hbase 链接发生异常：" + e);
        }
    }

    /**
     * 创建链接
     */
    public static void open(String tableName, String... columnFamily) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        if (!admin.tableExists(tablename)) {
            // 如果该表不存在，创建该表
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tablename);
            for (String family : columnFamily) {
                hTableDescriptor.addFamily(new HColumnDescriptor(family));
            }
            admin.createTable(hTableDescriptor);
        }
        log.info("tablename: { }" + tablename);
        table = connection.getTable(tablename);
    }

    /**
     * 关闭链接
     */
    public static void close() {
        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                log.error(" table 关闭异常：｛｝" + table);
            }
        }
        if (!flag) {
            try {
                connection.close();
            } catch (IOException e) {
                log.error(" connection 关闭异常：｛｝" + connection);
            }
        }
    }

    /**
     * 写一个数据
     */
    public static void putSingleData(String tableName, String rowKey ,String family, String columnName, String data) throws IOException {
        open(tableName, family);
        Put put = new Put(rowKey.getBytes());
        put.addColumn(family.getBytes(), columnName.getBytes(), Bytes.toBytes(data));
        table.put(put);
        table.close();
    }

    /**
     * 写一行数据
     */
    public static void writeRecord(String tableName, String rowKey, String family, Map<String, String> map) throws IOException {
        System.out.println("hbase 开始写数据啦，表名：" + tableName);
        open(tableName, family);
        //List<Put> list = new ArrayList<>();
        Put put = new Put(rowKey.getBytes());
        for (String key : map.keySet()) {
            System.out.println("key的值是===>" + key);
            System.out.println("value的值是====> :" + map.get(key));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(map.get(key)));
        }
        table.put(put);
        table.close();
    }


    public static String getData(String tableName, String rowKey, String famliyName, String column) throws IOException {
        open(tableName, famliyName);
        byte[] row = Bytes.toBytes(rowKey);
        Get get = new Get(row);
        Result result = table.get(get);
        byte[] resultValue = result.getValue(famliyName.getBytes(), column.getBytes());
        if (null == resultValue){
            return null;
        }
        table.close();
        return new String(resultValue);
    }

    public static void getMethod(Class clazz) {
        Class<? extends Class> cla = clazz.getClass();
        Method[] methods = cla.getDeclaredMethods();
        Field[] fields = cla.getDeclaredFields();

    }

}
