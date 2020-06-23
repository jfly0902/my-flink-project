package com.yskj.flink.task;

import com.yskj.flink.entity.RoomMember;
import com.yskj.flink.function.RoomMapFunction;
import com.yskj.flink.util.KafkaUtils;
import com.yskj.flink.util.PropertiesConstant;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/27 19:05
 */
public class HBaseSinkTask {

    /**
     * room的数据进过kafka过来
     * 数据具有延迟，设置半小时的WaterMark
     * 对于超过WaterMark的数据适用 outPutTag进行处理
     * 对于过来JSON数据，进行POJOType查询到HBase中，
     * HBASE 是数仓中的ODS的明细层
     * 处理后的数据设置窗口，全窗口函数
     * 将TOPN的数据放入Redis中，实时更新
     * 将所有的经过汇总的数据存入mysql中
     * 开启checkPoint,使用fsStateBackemd
     * 模式使用 仅此一次
     *
     *
     * @param args
     */

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(3 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointInterval(500L);
        config.setMaxConcurrentCheckpoints(3);
        env.setStateBackend(new FsStateBackend("hdfs://192.168.180.238:9000/flink"));

        Properties properties = KafkaUtils.buildKafkaProperties();

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>((String) properties.get(PropertiesConstant.TOPIC),
                new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromLatest();
        DataStream<RoomMember> input = env.addSource(kafkaConsumer).map(new RoomMapFunction())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<RoomMember>(Time.minutes(30)) {
                    @Override
                    public long extractTimestamp(RoomMember roomMember) {
                        return Long.parseLong(roomMember.getAddTime());
                    }
                });


        OutputTag<RoomMember> tag = new OutputTag<>("room-Tag");
        SingleOutputStreamOperator<Object> operator = input.filter(new FilterFunction<RoomMember>() {
            @Override
            public boolean filter(RoomMember roomMember) throws Exception {
                return roomMember.getRoomId().equals("");
            }
        }).keyBy(new KeySelector<RoomMember, String>() {
            @Override
            public String getKey(RoomMember roomMember) throws Exception {
                return "";
            }
        }).timeWindowAll(Time.hours(24), Time.hours(1))
                .allowedLateness(Time.minutes(30))
                .process(new ProcessAllWindowFunction<RoomMember, Object, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<RoomMember> iterable, Collector<Object> collector) throws Exception {

                    }
                });
        //operator.getSideOutput(tag).


    }



}
