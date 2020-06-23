package com.yskj.flink.task;

import com.yskj.flink.entity.AdClickEvent;
import com.yskj.flink.function.ClickCount;
import com.yskj.flink.function.ClickMapFunction;
import com.yskj.flink.function.ClickWindowFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/24 9:28
 */
public class BlackListTask {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.readTextFile("F://训练数据//taobaoApp//theme_click_log.csv")
                .map(new ClickMapFunction())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AdClickEvent>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(AdClickEvent adClickEvent) {
                        return Long.parseLong(Timestamp.valueOf(adClickEvent.getReach_time()).toString());
                    }
                }).keyBy(new KeySelector<AdClickEvent, String>() {
            @Override
            public String getKey(AdClickEvent adClickEvent) throws Exception {
                return adClickEvent.getTheme_id() + "," + adClickEvent.getItem_id();
            }
        }).timeWindow(Time.minutes(10), Time.minutes(5))
                .aggregate(new ClickCount(), new ClickWindowFunction())
                .keyBy("windowEnd")
                //.process()
                .print();

        env.execute("black list execute");
    }

}
