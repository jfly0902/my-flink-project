package com.yskj.flink.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @Author: xiang.jin
 * @Date: 2020/7/8 14:47
 */
@Slf4j
public class MysqlClient {

    private MysqlClient() {}

    private static Connection connection = null;

    private static BasicDataSource dataSource;

    public static Connection getMysqlConnection() {
        if (null == connection) {
            synchronized (MysqlClient.class) {
                if (null == connection) {
                    dataSource = new BasicDataSource();
                    dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
                    dataSource.setUrl(PropertyUtils.getProperty(CommonKey.MYSQL_URL));
                    dataSource.setUsername(PropertyUtils.getProperty(CommonKey.MYSQL_USERNAME));
                    dataSource.setPassword(PropertyUtils.getProperty(CommonKey.MYSQL_PASSWORD));
                    dataSource.setInitialSize(10);
                    dataSource.setMaxTotal(50);
                    dataSource.setMinIdle(2);
                    try {
                        connection = dataSource.getConnection();
                    } catch (SQLException e) {
                        log.error("DwsMsgSinkForMysql 获取 connection 发生异常,e:{}", e.getMessage());
                    }
                }
            }
        }
        return connection;
    }
}
