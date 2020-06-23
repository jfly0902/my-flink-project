package com.yskj.flink.task;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @Author: jinxiang
 * @Date: 2020/4/20 9:55
 */
public class ArrayTask {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.fromElements(1, 3, 4, 6, 7, 8, 9, 11).map(new MapFunction<Integer, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(Integer integer) throws Exception {
                Thread.sleep(4 * 1000L);
                return new Tuple2<String, Integer>(String.valueOf(integer), integer * 2);
            }
        }).print();

        env.execute("ArrayTask execute");

    }

}
