package com.yskj.flink.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yskj.flink.entity.WxChat;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/21 9:34
 */
public class AlertCEPTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.180.234:9092,192.168.180.235:9092,192.168.180.238:9092");
        properties.setProperty("zookeeper.connect", "192.168.180.234:2181,192.168.180.235:2181,192.168.180.238:2181");
        properties.setProperty("group.id", "wxChat");
        String topic = "test";
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromLatest();

        KeyedStream<WxChat, String> keyedStream = env.addSource(kafkaConsumer).uid("alert_kafka_source_id")
                .filter(Objects::nonNull).uid("alert_filter_null_id")
                .map(new MapFunction<String, WxChat>() {
                    @Override
                    public WxChat map(String s) throws Exception {
                        JSONObject record = JSON.parseObject(s);
                        System.out.println("输入的原始数据 =====》 ：" + s);
                        return new WxChat(
                                record.getString("wxId"),
                                record.getString("talker"),
                                record.getString("roomMsgWxId"),
                                record.getLong("chatCreateTime"),
                                record.getString("type"));
                    }
                }).uid("alert_map_transformation_id")
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<WxChat>(Time.seconds(5)) {
                    @SneakyThrows
                    @Override
                    public long extractTimestamp(WxChat wxChat) {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                        long time = wxChat.f3;
                        return time;
                    }
                }).uid("alert_waterMark_id")
                .keyBy(new KeySelector<WxChat, String>() {
                    @Override
                    public String getKey(WxChat wxChat) throws Exception {
                        return wxChat.f0 + "," + wxChat.f1 + "," + wxChat.f2;
                    }
                });

        /*Pattern<WxChat, WxChat> pattern = Pattern.<WxChat>begin("begin").where(new SimpleCondition<WxChat>() {
            @Override
            public boolean filter(WxChat wxChat) throws Exception {
                return wxChat.getType().equals("3");
            }
        }).followedBy("next").where(new SimpleCondition<WxChat>() {
            @Override
            public boolean filter(WxChat wxChat) throws Exception {
                return wxChat.getType().equals("3");
            }
        }).within(Time.seconds(10));*/

        Pattern<WxChat, WxChat> pattern = Pattern.<WxChat>begin("begin").where(new SimpleCondition<WxChat>() {
            @Override
            public boolean filter(WxChat wxChat) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                long time = wxChat.getChatCreateTime();
                long flag = wxChat.getChatCreateTime();
                return time >= flag;
            }
        }).followedBy("follow").where(new SimpleCondition<WxChat>() {
            @Override
            public boolean filter(WxChat wxChat) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                long time = wxChat.getChatCreateTime();
                long flag = wxChat.getChatCreateTime();
                return time >= flag;
            }
        }).times(9).within(Time.hours(5));


        //Pattern.<WxChat>begin("begin").where()

        PatternStream<WxChat> patternStream = CEP.pattern(keyedStream, pattern);

        patternStream.flatSelect(new PatternFlatSelectFunction<WxChat, WxChat>() {
            @Override
            public void flatSelect(Map<String, List<WxChat>> map, Collector<WxChat> out) throws Exception {
                System.out.println("value : =====> " + map.values());
                map.get("begin").forEach(out::collect);
                map.get("follow").forEach(out::collect);
            }
        }).print("数据： ====》");

        /*OutputTag<WxChat> tag = new OutputTag<WxChat>("side-outPut") {};

        SingleOutputStreamOperator<WxChat> operator = patternStream.flatSelect(tag,
                new PatternFlatTimeoutFunction<WxChat, WxChat>() {
                    @Override
                    public void timeout(Map<String, List<WxChat>> map, long l, Collector<WxChat> out) throws Exception {
                        List<WxChat> wxChats = map.get("begin");
                        System.out.println("timeOut 的 wxChat ： =====》 " + wxChats.toString());
                        wxChats.forEach(out::collect);
                    }
                },
                new PatternFlatSelectFunction<WxChat, WxChat>() {
                    @Override
                    public void flatSelect(Map<String, List<WxChat>> map, Collector<WxChat> out) throws Exception {
                        List<WxChat> beginWxChats = map.get("begin");
                        System.out.println("begin wxChat :" + beginWxChats.toString());
                        List<WxChat> endWxChats = map.get("next");
                        System.out.println("next wxChat :" + endWxChats.toString());
                        beginWxChats.forEach(out::collect);
                    }
                });

        operator.getSideOutput(tag).print("out_side ===> :");

        operator.print("picture =====> :");*/

        env.execute("Alert Job");
    }
}
