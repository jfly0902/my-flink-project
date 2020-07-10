package com.yskj.flink.task;


import com.yskj.flink.entity.DwdMsg;
import com.yskj.flink.entity.DwsMsg;
import com.yskj.flink.function.*;
import com.yskj.flink.sink.DwsMsgSinkForHBase;
import com.yskj.flink.sink.DwsMsgSinkForMysql;
import com.yskj.flink.util.CommonKey;
import com.yskj.flink.util.PropertyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * @Author: xiang.jin
 * @Date: 2020/5/11 16:41
 */
@Slf4j
public class RoomMsgCountProcessTimeTask {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.9.2");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        /*env.enableCheckpointing(18000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(15 * 60 * 1000L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new FsStateBackend(PropertyUtils.getProperty(CommonKey.FS_STATE_BACKEND)));*/

        Properties properties = new Properties();
        properties.setProperty(CommonKey.KAFKA_BOOTSTRAP_SERVER, PropertyUtils.getProperty(CommonKey.KAFKA_BOOTSTRAP_SERVER));
        properties.setProperty(CommonKey.KAFKA_ZOOKEEPER_CONNECT, PropertyUtils.getProperty(CommonKey.KAFKA_ZOOKEEPER_CONNECT));
        properties.setProperty("group.id", "room-msg-process-xiang");
        String topic = "msg";

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        //kafkaConsumer.setStartFromGroupOffsets();
        kafkaConsumer.setStartFromLatest();

        SingleOutputStreamOperator<DwdMsg> sourceStream = env.addSource(kafkaConsumer)
                .uid("source_id").name("source_id").setParallelism(1)
                .map(new RoomMsgMapFunction()).setParallelism(1)
                .uid("map_id").name("map_id")
                .returns(TypeInformation.of(DwdMsg.class));

        /*SingleOutputStreamOperator<DwdMsg> asyncSource = AsyncDataStream.unorderedWait(sourceStream,
                new AsyncFunctionForHBase(), 3000, TimeUnit.MICROSECONDS, 100).setParallelism(1)
                .uid("async-HBase-id").name("async-HBase-name");*/

        /**
         * 使用 processTime 就无须使用waterMark
         * waterMark是基于事件的
         */
         SingleOutputStreamOperator<DwsMsg> streamOperator = sourceStream.keyBy(new KeySelector<DwdMsg, String>() {
                    @Override
                    public String getKey(DwdMsg dwdMsg) throws Exception {
                        log.info(" ===> key:{}", dwdMsg.getChatDate() + "," + dwdMsg.getTalker() + "," + dwdMsg.getRoomMsgWxId());
                        return dwdMsg.getChatDate() + "," + dwdMsg.getTalker() + "," + dwdMsg.getRoomMsgWxId();
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
                 /**
                  * 数据倾斜，解决数据倾斜以后，再将分发到不同key的数据
                  * 再进一步进行聚合，因此在 agg的时候需要自定也一个 Accumulator
                  */
                .aggregate(new RoomMsgAggCountFunction(), new ResultWindowFunction())
                .returns(PojoTypeInfo.of(DwsMsg.class))
                .setParallelism(1)
                .uid("agg_wxChat_ID")
                .keyBy(new KeySelector<DwsMsg, String>() {
                    @Override
                    public String getKey(DwsMsg dwsMsg) throws Exception {
                        return dwsMsg.getTalker() + "," + dwsMsg.getRoomMsgWxId() + "," + dwsMsg.getWeChatTime() + "," + dwsMsg.getWindowEnd();
                    }
                })
                // 处理分发的数据
                .process(new DataSkewAggProcessFunction())
                .uid("process_dataSkew_id")
                .name("process_dataSkew_name")
                .setParallelism(1);

        streamOperator.addSink(new SinkFunction<DwsMsg>() {
                    @Override
                    public void invoke(DwsMsg value, Context context) throws Exception {
                        System.out.println("dw ====>:" + value.toString());
                    }
                });

        //streamOperator.addSink(new DwsMsgSinkForHBase()).uid("dws-room-sink-hbase-id").name("dws-room-sink-hbase-name").setParallelism(1);

        //streamOperator.addSink(new DwsMsgSinkForMysql()).uid("dws-room-sink-mysql-id").name("dws-room-sink-mysql-name").setParallelism(1);

        env.execute("Room Msg process time Task Job");
    }

}
