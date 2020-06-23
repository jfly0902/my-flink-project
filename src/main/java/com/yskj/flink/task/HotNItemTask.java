package com.yskj.flink.task;

import com.yskj.flink.entity.UserBehavior;
import com.yskj.flink.function.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HotNItemTask {

    private static final int topSize = 3;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //一小时内点击量最多的前 N 个商品 定义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 创建一个 state存储，默认是memory的
        //env.setStateBackend(new HeapKeyedStateBackend<>());

        // 设置数据源是kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.180.239:9092");
        properties.setProperty("zookeeper.connect", "192.168.180.239:2181");
        properties.setProperty("group.id", "hot");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("test01", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest(); // 最早开始读
        kafkaConsumer.setStartFromLatest();  // 最新位置开始读
        kafkaConsumer.setStartFromTimestamp(15945905); //从时间戳大于或者等于指定的时间戳开始读取
        kafkaConsumer.setStartFromGroupOffsets(); // 从group.id位置开始读取，如果没有根据 auto.offset.reset设置读取

        Map<KafkaTopicPartition, Long> specificStartupOffsets = new HashMap<>();
        specificStartupOffsets.put(new KafkaTopicPartition("test01", 0), 1L);
        specificStartupOffsets.put(new KafkaTopicPartition("test01", 1), 0L);
        kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);

        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointTimeout(5 * 60 * 1000L);
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setMinPauseBetweenCheckpoints(500);

        env.setStateBackend(new FsStateBackend("hdfs://192.168.180.238:9000/flink/"));

        /**
         * 使用producer 自定义一个序列化的类
         * 需要在 env中需要将这个序列化的schema注册到env中
         * env.registerTypeWithKryoSerializer("序列化类的.calss", "使用的是哪种序列化");
         */

        //FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("test01", new SimpleStringSchema(), properties);
        // 设置watermark

        // 加载本地文件
        URL url = HotNItemTask.class.getClassLoader().getResource("/UserBehavior.csv");
        System.out.println(url);
        Path filePath = Path.fromLocalFile(new File("src/main/resources/UserBehavior.csv"));
        // 生成POJO
        // 指定 实体类的序列化类型
        PojoTypeInfo<UserBehavior> pojoTypeInfo = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);

        // 由于java 反射的是顺序不一定，因此显示指定顺序
        String[] filed = {"userId", "itemId", "categoryId", "behavior", "timestamp"};

        PojoCsvInputFormat<UserBehavior> inputFormat = new PojoCsvInputFormat<>(filePath, pojoTypeInfo, filed);

        // 创建一个输入源
        DataStream<UserBehavior> streamOperator = env.createInput(inputFormat, pojoTypeInfo)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.timestamp * 1000L ;
            }
        });

        // 侧输出流
        OutputTag<UserBehavior> tag = new OutputTag<>("top-tag", PojoTypeInfo.of(UserBehavior.class));

        // 数据中存在点击/购买等行为，统计的是点击事件，因此需要过滤拿到pv的数据
        SingleOutputStreamOperator<String> operator = streamOperator.filter(new FilterFunction<UserBehavior>() {

            private static final long serialVersionUID = 1L;

            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.getBehavior().equals("pv");
            }
        })
                //拿到特定的数据后设置一个窗口，窗口的时间是1个小时，设置滑动窗口的大小为5分钟,最终显示的数据是最近一个小时的TOPN
                .keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(tag)
                // 预先聚合,现在我们得到了每个商品在每个窗口的点击量的数据流。
                .aggregate(new CountFunction(), new WindowResultFunction())
                // 统计每个窗口下最热门的商品，我们需要再次按窗口进行分组
                .keyBy("windowEnd")
                // 使用 processFunction 获取每个窗口TOPN
                .process(new TopNItemsFunction(topSize));

        /*DataStreamUtils.reinterpretAsKeyedStream(operator, new KeySelector<String, Object>() {
            @Override
            public Object getKey(String s) throws Exception {
                return null;
            }
        });*/

        // 输出到控制台
        operator.print();
        //operator.addSink(kafkaProducer);

        // 获取侧输出流 进行处理
        operator.getSideOutput(tag).map(new MapFunction<UserBehavior, Object>() {
            @Override
            public Object map(UserBehavior userBehavior) throws Exception {

                return null;
            }
        });

        env.execute("Hot Items Job");
    }

}
