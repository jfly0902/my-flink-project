package com.yskj.flink.task;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.yskj.flink.entity.AppUserBehavior;
import com.yskj.flink.function.AppMarketingWindowFunction;
import com.yskj.flink.function.AppUserBehaviorFunction;
import com.yskj.flink.source.AppSourceData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/23 17:34
 */
@Slf4j
public class AppChannelTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(180000);
        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointTimeout(15 * 60 * 1000L);
        config.setMinPauseBetweenCheckpoints(500L);
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setPreferCheckpointForRecovery(true);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.180.239:9092");
        properties.setProperty("zookeeper.connect", "192.168.180.239:2181");
        properties.setProperty("group.id", "room02");

        String topic = "test01";

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();

        DataStream<String> streamOperator = env.addSource(new AppSourceData())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(String s) {
                String[] split = s.split(",");
                return Long.parseLong(split[split.length -1]);
            }
        });

        streamOperator.map(new AppUserBehaviorFunction())
                .filter(new FilterFunction<AppUserBehavior>() {
                    @Override
                    public boolean filter(AppUserBehavior appUserBehavior) throws Exception {
                        return !appUserBehavior.getBehavior().equals("uninstall");
                    }
                })
                .keyBy(new KeySelector<AppUserBehavior, String>() {
                    @Override
                    public String getKey(AppUserBehavior appUserBehavior) throws Exception {
                        return appUserBehavior.getChannel() + "," + appUserBehavior.getBehavior();
                    }
                })
                .timeWindow(Time.seconds(60), Time.seconds(10))
                .process(new AppMarketingWindowFunction())
                .print();

        env.execute("App Marketing");
    }

}
