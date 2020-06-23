package com.yskj.flink.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yskj.flink.entity.WxChat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;
import java.util.Properties;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/20 15:04
 */
public class WxChatCountTask {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.180.234:9092,192.168.180.235:9092,192.168.180.238:9092");
        properties.setProperty("zookeeper.connect", "192.168.180.234:2181,192.168.180.235:2181,192.168.180.238:2181");
        properties.setProperty("group.id", "wxChat");
        String topic = "test";
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromLatest();

        final OutputTag<WxChat> roomTag = new OutputTag<WxChat>("room_chat") {};
        final OutputTag<WxChat> privateTag = new OutputTag<WxChat>("private_chat") {};

        SingleOutputStreamOperator<WxChat> sideOutputStreamOperator = env.addSource(kafkaConsumer).filter(Objects::nonNull).uid("wxChat_kafka_source_id")
                .map(new MapFunction<String, WxChat>() {
                    @Override
                    public WxChat map(String json) throws Exception {
                        JSONObject record = JSON.parseObject(json);

                        return new WxChat(
                                record.getString("wxId"),
                                record.getString("talker"),
                                record.getString("roomMsgWxId"),
                                record.getString("chatCreateTime"),
                                record.getString("type"));
                    }
                }).uid("wxChat_json_HBase_id")
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<WxChat>(Time.minutes(45)) {
                    @Override
                    public long extractTimestamp(WxChat wxChat) {
                        return Long.parseLong(wxChat.f3);
                    }
                }).process(new ProcessFunction<WxChat, WxChat>() {
                    @Override
                    public void processElement(WxChat wxChat, Context context, Collector<WxChat> out) throws Exception {
                        if (wxChat.f1.contains("@chatroom")) {
                            // 侧输出流输出
                            context.output(roomTag, wxChat);
                        } else {
                            context.output(privateTag, wxChat);
                        }
                    }
                }).uid("wxChat_side_source_id");


        // 对私聊进行处理
        sideOutputStreamOperator.getSideOutput(privateTag).print();

        // 对群聊进行处理
        sideOutputStreamOperator.getSideOutput(roomTag).print();

    }
}
