package com.yskj.flink.task;

import com.yskj.flink.entity.UserBehavior;
import com.yskj.flink.function.UVTriggerFunction;
import com.yskj.flink.function.UvBloomFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/24 16:57
 */
public class BloomFilterTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Path path = Path.fromLocalFile(new File("src/main/resources/UserBehavior.csv"));
        PojoTypeInfo<UserBehavior> pojoTypeInfo = (PojoTypeInfo<UserBehavior>) TypeInformation.of(UserBehavior.class);
        String[] filed = {"userId", "itemId", "categoryId", "behavior", "timestamp"};

        PojoCsvInputFormat<UserBehavior> inputFormat = new PojoCsvInputFormat<>(path, pojoTypeInfo, filed);

        env.createInput(inputFormat, pojoTypeInfo)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior) {
                        return userBehavior.timestamp * 1000L;
                    }
                }).filter(userBehavior -> userBehavior.behavior.equals("pv"))
                .map(data -> ("key : " + data.userId))
                .keyBy(0)
                .timeWindow(Time.hours(1))
                .trigger(new UVTriggerFunction())
                .process(new UvBloomFunction())
                .print();

        env.execute("bloom uv");
    }

}
