package com.yskj.flink.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author: xiang.jin
 * @Date: 2020/7/8 13:44
 */
@Slf4j
public class HBaseClient {

    private static Connection connection = null;

    private HBaseClient() {}

    public static Connection getHBaseConnection () {
        if (null == connection) {
            synchronized (HBaseClient.class) {
                if (null == connection) {
                    Configuration configuration = HBaseConfiguration.create();
                    configuration.set(HConstants.ZOOKEEPER_QUORUM, PropertyUtils.getProperty(CommonKey.HBASE_ZOOKEEPER_QUORUM));
                    configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, PropertyUtils.getProperty(CommonKey.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT));
                    configuration.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, Integer.parseInt(PropertyUtils.getProperty(CommonKey.HBASE_CLIENT_OPERATION_TIMEOUT)));
                    configuration.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, Integer.parseInt(PropertyUtils.getProperty(CommonKey.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD)));
                    configuration.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, Integer.parseInt(PropertyUtils.getProperty(CommonKey.HBASE_RPC_TIMEOUT)));
                    ExecutorService pool = Executors.newFixedThreadPool(10);
                    try {
                        connection = ConnectionFactory.createConnection(configuration, pool);
                        log.info(" HBase connection success");
                    } catch (IOException e) {
                        log.error(" ===> HBase 获取连接发生异常 <=====,异常信息 e:{}", e.getMessage());
                    }
                }
            }
        }
        return connection;
    }
}
