package com.yskj.flink.task;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 *  窗口 + 水印  处理数据乱序问题
 */
public class WindowWaterMark {

    public static void main(String[] args) throws Exception {
        // 获取flink环境上下文
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //environment.setParallelism(1);

        // 设置为EvenTime事件类型
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置水印生成间隔 100ms
        environment.getConfig().setAutoWatermarkInterval(100);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.180.234:9092,192.168.180.235:9092,192.168.180.238:9092");
        properties.setProperty("zookeeper.connect", "192.168.180.234:2181,192.168.180.235:2181,192.168.180.238:2181");
        properties.setProperty("group.id", "test");

        String topic = "test";
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        //kafkaConsumer.setStartFromGroupOffsets();
        kafkaConsumer.setStartFromLatest();

        // 接收数据，并为每条数据设置水印
        DataStream<String> socketTextStream = environment
                /**
                 * 使用 socketTextStream 源码走读可以看出，默认给的并行度是1
                 * 还有一个formCollect() 都是默认赋值并行度为一
                 * 使用kafka 等其他的会source会根据你cpu的核数来确定你的默认的并行度
                 */
                //.socketTextStream("192.168.180.234", 9999)
                .addSource(kafkaConsumer)
                /*.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(String s) {
                System.out.println("输入的数据：" + s);
                String[] split = s.split(",");
                long timeStamp  = Long.parseLong(split[1]);
                return timeStamp;
            }
        });*/
                .assignTimestampsAndWatermarks(new generateWaterMark());
        // 业务逻辑处理
        socketTextStream.map(new MyMapFunction())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                /*.process(new ProcessWindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple key, Context context, Iterable<Tuple2<String, Long>> input, Collector<String> collector) throws Exception {
                        long currentWatermark = context.currentWatermark();
                        System.out.println("当前的 currentWatermark：" + currentWatermark);
                        Tuple2<String, Long> tuple2 = input.iterator().next();
                        collector.collect(tuple2.f0 + "," + tuple2.f1);
                    }
                })*/
                .minBy(1)
                .print("sdfsdfgs:");

        environment.execute(WindowWaterMark.class.getSimpleName());
    }

    public static class MyMapFunction implements MapFunction<String, Tuple2<String,Long>>{

        @Override
        public Tuple2<String, Long> map(String s) throws Exception {
            System.out.println("map :" + s);
            String[] split = s.split(",");
            return new Tuple2<>(split[0],Long.parseLong(split[1]));
        }
    }

    public static class generateWaterMark implements AssignerWithPeriodicWatermarks<String>{

        private Long currentTimeStamp = 0L;
        //设置允许乱序时间 5s
        private Long maxOutOfOrderness = 5000L;

        /**
         * 从数据源中抽取时间戳
         * @param s
         * @param l
         * @return
         */
        @Override
        public long extractTimestamp(String s, long l) {
            String[] split = s.split(",");
            long timeStamp  = Long.parseLong(split[1]);
            currentTimeStamp = Math.max(timeStamp,currentTimeStamp);
            System.err.println(s + ",EventTime:" + timeStamp + ",watermark:" + (currentTimeStamp - maxOutOfOrderness));
            return timeStamp;
        }

        /**
         * 发射时间水印
         * @return
         */
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimeStamp - maxOutOfOrderness);
        }
    }
}
