package com.yskj.flink.task;

import com.yskj.flink.entity.TalkerEntity;
import com.yskj.flink.entity.UserTalkerEntity;
import com.yskj.flink.function.CountAgg;
import com.yskj.flink.function.JsonMapFunction;
import com.yskj.flink.window.TopNHotTalker;
import com.yskj.flink.window.WindowResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

/**
 * @Author: jinxiang
 * @Date: 2020/4/20 10:29
 */
public class KafkaTask {

    private static final int topSize = 5;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.180.239:9092");
        properties.setProperty("zookeeper.connect", "192.168.180.239:2181");
        properties.setProperty("group.id", "room01");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("test01", new SimpleStringSchema(), properties);
        kafkaSource.setStartFromLatest();
        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaSource);
        kafkaDataStream.map(new JsonMapFunction()).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TalkerEntity>() {
            @Override
            public long extractAscendingTimestamp(TalkerEntity talkerEntity) {
                return Long.parseLong(talkerEntity.getChatCreateTime());
            }
        }).keyBy(TalkerEntity::getTalker)
                .timeWindow(Time.seconds(60), Time.seconds(5))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy("windowEnd")         // 按照窗口进行分组
                .process(new TopNHotTalker(topSize)).flatMap(new FlatMapFunction<List<UserTalkerEntity>, String>() {

                    @Override
                    public void flatMap(List<UserTalkerEntity> list, Collector<String> out) throws Exception {
                        list.forEach(userTalkerEntity -> {
                            out.collect(userTalkerEntity.getTalker() + " : " + userTalkerEntity.getCount());
                        });
                    }
                }).print();
                // sink 这里可以定义sink到kafka中去
                //.addSink();

        env.execute(" kafkaSource");
    }

}
