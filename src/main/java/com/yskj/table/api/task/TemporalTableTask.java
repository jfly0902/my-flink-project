package com.yskj.table.api.task;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: xiang.jin
 * @Date: 2020/6/2 13:56
 */
public class TemporalTableTask {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
        ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
        ratesHistoryData.add(Tuple2.of("Euro", 114L));
        ratesHistoryData.add(Tuple2.of("Yen", 1L));
        ratesHistoryData.add(Tuple2.of("Euro", 116L));
        ratesHistoryData.add(Tuple2.of("Euro", 119L));

        DataStreamSource<Tuple2<String, Long>> dataStreamSource = env.fromCollection(ratesHistoryData);

        Table ratesHistory  = tableEnvironment.fromDataStream(dataStreamSource, "r_currency, r_rate, r_proctime.proctime");

        tableEnvironment.createTemporaryView("RatesHistory", ratesHistory);

        /**
         * 创建和注册一个 function
         * 定义 "r_proctime" 为 时间属性，并且定义 "r_currency" 为 主键
         */
        TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("r_proctime", "r_currency");

        tableEnvironment.registerFunction("Rates", rates);

    }

}
