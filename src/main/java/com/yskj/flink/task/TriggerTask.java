package com.yskj.flink.task;

import com.google.common.hash.BloomFilter;
import com.yskj.flink.entity.WxChat;
import com.yskj.flink.entity.WxChatHistoryCloud;
import com.yskj.flink.function.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * @Author: xiang.jin
 * @Date: 2020/4/30 15:51
 */

/**
 * 我的群聊
 * 第一：需要统计的是今天活跃客户数（UV）和近7天的活跃用户数（UV）
 * 第二： 群聊排行榜，客户发言的条数(PV)
 * 根据这两个指标，有三个ID
 * 业务员微信号  : wxid
 * 群聊房间    ： Room_id
 * 客户的微信号： khid
 *
 * 而第一个需求，使用key 为 wxid+room_id 为key确定一个业务人员的群聊，统计 发言的客户微信号(UV)
 * 第二个需求： wxid+room_id+khid 为key，统计客户发言的条数（PV）
 *
 * ods层在kafka ---->  使用flink算子，将kafka的数据通过map/flatMap将数据明细放入 HBase中（DW明细层）
 *
 * 统计客户发言的条数，使用 wxId+room_id+khid 生成一个唯一的主键，在Hbase做一个计算器，统计客户的每天发言条数。
 * 同理统计一天的活跃人数，根据客户的发言来统计UV，这样就可以省掉去统计UV的算法操作，将计数器的结果放入HBASE中
 * 由此可以这也可以轻度聚合到HBASE中，这是实时的结果，如果接口放回较慢，可以将结果在redis中存放一份（使用Bloom过滤器来判断用户）
 *
 *
 * 开通一条离线查询的结果，来验证实时数据的准确性
 */


public class TriggerTask {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(18000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig config = env.getCheckpointConfig();
        env.setStateBackend(new RocksDBStateBackend("hdfs:///flink/savepoints"));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.180.239:9092");
        properties.setProperty("zookeeper.connect", "192.168.180.239:2181");
        properties.setProperty("group.id", "hot");

        String topic = "test01";

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        /**
         * kafka的checkPoint/SavePoint 状态容错，重新消费kafka的数据跟以下kafka的配置无关
         */
        // 从最晚的数据开始消费
        kafkaConsumer.setStartFromLatest();
        // 从最早的数据开始消费
        kafkaConsumer.setStartFromEarliest();
        // 默认的方式，指定的group上次消费的位置开始消费，所以必须配置group.id参数
        kafkaConsumer.setStartFromGroupOffsets();
        // Flink从topic中指定的offset开始，这个比较复杂，需要手动指定offset
        // kafkaConsumer.setStartFromSpecificOffsets();
        // 指定的时间戳开始消费
        kafkaConsumer.setStartFromTimestamp(1559801580000l);

        // 禁用或启用偏移提交默认情况下，行为是true,默认情况下是启动的
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        PojoTypeInfo<WxChatHistoryCloud> pojoTypeInfo = (PojoTypeInfo<WxChatHistoryCloud>) TypeInformation.of(WxChatHistoryCloud.class);

        PojoTypeInfo<WxChat> pojo = (PojoTypeInfo<WxChat>) TypeInformation.of(WxChat.class);

        SingleOutputStreamOperator<String> sourceStream = env.addSource(kafkaConsumer).uid("wxChat_kafka_source_id");
        DataStream<Map<String, String>> kafkaStream = AsyncDataStream.unorderedWait(sourceStream,
                new WxChat2HBaseMapFunction(), 1000, TimeUnit.MILLISECONDS, 100)
                .uid("wxChat_Async_HBase_id")
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Map<String, String>>(Time.seconds(30)) {
                    @Override
                    public long extractTimestamp(Map<String, String> map) {
                        return Long.parseLong(map.get("chatCreateTime"));
                    }
                }).uid("wxChat_WaterMark_Id");

        OutputTag<Map<String, String>> wxChatTag = new OutputTag<>("weChat-stat");

        // @chatroom
        SingleOutputStreamOperator<Map<String, Integer>> resultStream = kafkaStream.filter(new FilterFunction<Map<String, String>>() {
            @Override
            public boolean filter(Map<String, String> map) throws Exception {
                return map.get("talker").contains("@chatroom");
            }
        }).uid("wxChat_filter_chatRoom_Id")
                .keyBy(new KeySelector<Map<String, String>, String>() {

            @Override
            public String getKey(Map<String, String> map) throws Exception {
                return map.get("wxId") + "," + map.get("talker") + "," + map.get("roomMsgWxId");
            }
        }).window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))

                .allowedLateness(Time.minutes(5))
                // 记录迟到的数据
                .sideOutputLateData(wxChatTag)
                /**
                 * ContinuousEventTimeTrigger 一个窗口基于EventTime，可以多次触发，每2min触发一次
                 * PurgingTrigger 当nested(嵌套的) trigger触发时，额外的清除窗口中的中间的状态。
                 * 如果数据库的操作没有Update操作，只能进行append操作，则使用 PurgingTrigger
                 * PurgingTrigger 再trigger触发的时候，会将state进行清零
                 * 而ContinuousEventTimeTrigger的在触发trigger的时候不会将state的中间状态清零
                 *
                 * 因为HBASE 支持Update操作因此这里不需要使用 PurgingTrigger
                 * 因此可以考虑设置TTL或者 使用evictor只处理最近 2 min的数据
                 * 考虑到性能问题最好是设置一个TTL
                 */
                .trigger(ContinuousEventTimeTrigger.of(Time.minutes(2)))
                /**
                 * TimeEvictor 筛选只要最近一段时间的数据进行计算
                 * 一旦使用了 evictor 中间状态就需要使用listState来保存数据了
                 * 因为只有list可以保存一条条的中间数据
                 *
                 */
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                //.aggregate(new WxChatRecordCountFunction(), )
                .process(new ReduceWxChatFunction())
                .uid("wxChat_window_process_Id");


        /**
         * 统计一天的的聊天的数据
         * 使用滚动窗口，因为flink默认的是UTC时区，我们是东8区，因此需要 -8
         * 自定义Trigger,在一天时间内每隔5分钟trigger 一次
         * evictor()可以增量统计数据，最后将一天的统计的数据写入hbase中
         */
        //operator.keyBy(0).window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                //.trigger(PurgingTrigger.of (ContinuousEventTimeTrigger.of(Time.minutes(5))))
                //.evictor()
                //.aggregate()

        //resultStream.addSink(new HBaseSinkFunction()).uid("wxChat_Sink_HBase_id");

        resultStream.getSideOutput(wxChatTag).addSink(new SideOutPutFunction()).uid("wxChat_side_HBase_Id");

        env.execute(" WxChat Flink Job Execute ");
    }

}
