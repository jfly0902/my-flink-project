package com.yskj.table.api.task;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @Author: xiang.jin
 * @Date: 2020/6/9 17:38
 */
public class EpidemicControlTask {

    /**
     * 近 14 天新增报告本地感染确诊病例
     * 一级：100例或者爆发性 20例以上
     * 二级：10-99 或者 爆发性 5-19
     * 三级：1-9 或者 爆发性 1-4
     * 四级：0
     *
     * kafka 数据结构  城市、时间、数量、类型(0代表普通，1代表爆发性)、其他
     * {
     *     "city":"武汉",
     *     "ts":"2020-02-12 15:35:67",
     *     "cn":"2",
     *     "type":"1|0",
     *     "other":"other"
     * }
     *
     * 计算结果表
     * city | ts | level | other
     * 武汉 | 2020-02-12 15:35:67 | 3 | other
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceTableSql = "CREATE TABLE sourceTable (" +
                "city VARCHAR," +
                "ts TIMESTAMP," +
                "cn INT," +
                "type INT," +
                "other VARCHAR," +
                "WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
                ")WITH(" +
                "\'connector.type\' = \'kafka\'," +
                "\'connector.version\' = \'0.11\'," +
                "\'connector.topic\' = \'epidemic\'," +
                "\'connector.properties.bootstrap.servers\' = \'192.168.180.234:9092,192.168.180.235:9092,192.168.180.238:9092\'," +
                "\'connector.properties.group.id\' = \'city\'," +
                "\'format.type\' = \'json\')" ;

        String sinkTableSql = "CREATE TABLE sinkTable (" +
                "city VARCHAR," +
                "ts TIMESTAMP," +
                "level INT," +
                "other VARCHAR" +
                ")WITH(" +
                "\'connector.type\' = \'jdbc\'," +
                "\'connector.url\' = \'jdbc:mysql://localhost:3306/flink\'," +
                "\'connector.table\' = \'epidemic\'," +
                "\'connector.username\' = \'root\'," +
                "\'connector.password\' = \'123456\'," +
                // 此处设定每秒刷新一次到 mysql数据库中，默认是有5000条刷一次，后期flink社区会更改这个默认的设置
                "\'connector.writer.flush.interval\' = \'1s\')" ;


        /**
         * sum(cn) OVER (partition BY city ORDER BY ts RANG BETWEEN INTERVAL '14' DAY PRECEDING AND CURRENT ROW) AS cnt
         * sum(type) OVER (partition BY city ORDER BY ts RANG BETWEEN INTERVAL '14' DAY PRECEDING AND CURRENT ROW) AS cnt
         *
         * 因此将这两种复杂的判断的逻辑写一个 UDF levelUDF
         *
         * 因此聚合的sql
         */
        String sql = "INSERT INTO sinkTable" +
                "select city, ts," +
                "levelUDF(" +
                "sum(cn) OVER (partition BY city ORDER BY ts RANG BETWEEN INTERVAL '14' DAY PRECEDING AND CURRENT ROW) AS cnt" +
                "sum(type) OVER (partition BY city ORDER BY ts RANG BETWEEN INTERVAL '14' DAY PRECEDING AND CURRENT ROW) AS cnt" +
                ") AS level," +
                "other from sourceTable";


        tEnv.sqlUpdate(sourceTableSql);
        tEnv.sqlUpdate(sinkTableSql);
        tEnv.sqlUpdate(sql);

        env.execute("sql");
    }
}
