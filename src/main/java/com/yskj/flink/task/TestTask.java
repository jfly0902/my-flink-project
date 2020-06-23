package com.yskj.flink.task;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Objects;
import java.util.Properties;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/26 11:13
 */
public class TestTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.enableCheckpointing(18000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig config = env.getCheckpointConfig();
        config.setMaxConcurrentCheckpoints(3);
        config.setCheckpointTimeout(15 * 60 * 1000L);
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setMinPauseBetweenCheckpoints(500);
        config.setPreferCheckpointForRecovery(true);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.180.234:9092,192.168.180.235:9092,192.168.180.238:9092");
        properties.setProperty("zookeeper.connect", "192.168.180.234:2181,192.168.180.235:2181,192.168.180.238:2181");
        properties.setProperty("group.id", "wxChat");

        String topic = "test";
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        //kafkaConsumer.setStartFromGroupOffsets();
        kafkaConsumer.setStartFromLatest();
        env.addSource(kafkaConsumer).setParallelism(1).uid("source_id")
                .filter(Objects::nonNull).uid("filter_id").setParallelism(2)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        return s;
                    }
                }).uid("map_id").setParallelism(2)
                .print("job start").uid("print_id").setParallelism(1);
        env.execute("test job");
    }
}
