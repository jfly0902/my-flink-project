package com.yskj.flink.task;

import com.yskj.flink.entity.WxChat;
import com.yskj.flink.function.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.eclipse.jetty.util.StringUtil;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/11 9:24
 */
public class WxChat2HBaseTask {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.9.2");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.enableCheckpointing(18000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig config = env.getCheckpointConfig();
        config.setMaxConcurrentCheckpoints(3);
        config.setCheckpointTimeout(15 * 60 * 1000L);
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setMinPauseBetweenCheckpoints(500);
        config.setPreferCheckpointForRecovery(true);

        // 设置状态后端
        env.setStateBackend(new RocksDBStateBackend("hdfs://node1:9000/flink"));

        // 延迟数据侧输出流输出
        OutputTag<Map<String, String>> wxChatTag = new OutputTag<>("wxChat-state", TypeInformation.of(new TypeHint<Map<String,String>>(){}));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.180.234:9092,192.168.180.235:9092,192.168.180.238:9092");
        properties.setProperty("zookeeper.connect", "192.168.180.234:2181,192.168.180.235:2181,192.168.180.238:2181");
        properties.setProperty("group.id", "wxChat");
        
        String topic = "wxChat";

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        //kafkaConsumer.setStartFromGroupOffsets();
        kafkaConsumer.setStartFromLatest();

        DataStream<Map<String, String>> streamOperator = env.addSource(kafkaConsumer)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        System.out.println("kafka 进来的json的数据 ==========>:" + s);
                        return StringUtils.isNotEmpty(s);
                    }
                }).uid("wxChat_kafka_source_id")
                .map(new KafkaJson2HBaseFunction())
                .uid("ods_2HBase_function_id")
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Map<String, String>>(Time.minutes(45)) {
                    @Override
                    public long extractTimestamp(Map<String, String> map) {
                        System.out.println("assignTimestampsAndWatermarks :" + map);
                        long chatCreateTime = Long.parseLong(map.get("chatCreateTime")) * 1000;
                        System.out.println("============> chatCreateTime:" + chatCreateTime);
                        return chatCreateTime;
                    }
                }).uid("waterMark_function_id");


        SingleOutputStreamOperator<Map<String, String>> resultStream = streamOperator
                // 筛选出是群聊的消息
                .filter((FilterFunction<Map<String, String>>) map -> {
                    System.out.println("=====> 刷选出群聊的信息：" + map + "<==========");
                    return map.get("talker").contains("@chatroom");
                })
                .uid("filter_chatRoom_id")
                // 获取key, key 用 wxId + talker + roomMsgWxId 作为key，只需要记录次数放入hbase即可
                .keyBy(new KeySelector<Map<String, String>, String>() {
                    @Override
                    public String getKey(Map<String, String> map) throws Exception {
                        System.out.println("=================> 获取key :" + map);
                        return map.get("wxId") + "," +map.get("talker") + "," + map.get("roomMsgWxId");
                    }
                })
                // 统计一天的数据，所以使用滚动窗口，因为flink默认的时间是 UTC 时区，因此我们使用的是 中8区， 因此需要 -8个小时
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                // 允许数据延迟的时间是
                //.allowedLateness(Time.minutes(5))
                // 处理延迟数据
                .sideOutputLateData(wxChatTag)
                // 对于一天的窗口自定义触发机制, 多次触发，每5分钟触发一次
                // 由于HBase 支持update的操作，因此可以直接使用ContinuousEventTimeTrigger
                // 如果有些数据库不支持update操作，只支持append,
                // 因此可以在 PurgingTrigger.of(ContinuousEventTimeTrigger.of(Time.minutes(5)))
                // 外部包一层 PurgingTrigger.of(), 他会将之前的state的数据给清理追加到append中。
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                // 因为给的是一天的数据处理，没5分钟触发一次，因此只算增量的数据，过滤掉之前算过的数据
                // 因此保留最近5分钟的数据即可
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                // 对数据进行处理
                .process(new ProcessWindowFunction<Map<String, String>, Map<String, String>, String, TimeWindow>() {

                    // 统计key 的 pv
                    private transient ValueState<Integer> pvValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.minutes(3))
                                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();

                        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("pv-state", Integer.class);
                        valueStateDescriptor.enableTimeToLive(ttlConfig);
                        pvValueState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void process(String key, Context context, Iterable<Map<String, String>> input, Collector<Map<String, String>> out) throws Exception {

                        System.out.println("=============> 当前的waterMark：" + context.currentWatermark());


                        System.out.println("=============> WxChatStatisticsFunction is key:" + key);
                        // 获取当前pv的value值
                        Integer value = pvValueState.value();

                        if (value == null) {
                            value = 0;
                        }
                        System.out.println(" =========> pv的value 的值：" + value);
                        Iterator<Map<String, String>> iterator = input.iterator();
                        while (iterator.hasNext()) {
                            System.out.println("=============> WxChatStatisticsFunction iterator:" + iterator.next());
                            value += 1;
                        }
                        pvValueState.update(value);
                        /**
                         * 对侧输出的数据进行输出
                         */
                        //context.output();

                        Map<String, String> map = new HashMap<>();
                        String[] split = key.split(",");
                        for (int i = 0; i < split.length; i++) {
                            map.put(split[i], split[i]);
                        }
                        map.put("count", value.toString());

                        System.out.println("===========> 最后统计的结果key：" + key + ": value: " + value + "<================");
                        out.collect(map);
                    }
                })
                .uid("wxChat_result_Stream_Id");

        // 将结果写入到HBase的DW中
        resultStream.addSink(new HBaseDWSinkFunction()).uid("wxChat_dwHBase_id");
        // 获取延迟数据
        resultStream.getSideOutput(wxChatTag).addSink(new SideSinkFunction()).uid("wxChat_Tag_HBase_id");

        env.execute("WxChat ETL JOB");
    }
}
