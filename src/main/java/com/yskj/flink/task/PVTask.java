package com.yskj.flink.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yskj.flink.entity.WxChat;
import com.yskj.flink.function.*;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Util.println;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/11 16:41
 */
public class PVTask {

    public static void main(String[] args) throws Exception {

        //System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.9.2");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         *  对于多并行度，官网给出的图如下，也就是对于多并行度来说，会根据所有并行度中最低的水位线来触发窗口操作
         */
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.enableCheckpointing(18000L, CheckpointingMode.EXACTLY_ONCE);
        //CheckpointConfig config = env.getCheckpointConfig();
        /*config.setMaxConcurrentCheckpoints(3);
        config.setCheckpointTimeout(15 * 60 * 1000L);
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setMinPauseBetweenCheckpoints(500);
        config.setPreferCheckpointForRecovery(true);*/

        // 设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://node1:9000/flink/pvTask"));

        // OutputTag<Map<String, String>> wxChatTag = new OutputTag<>("wxChat-state", TypeInformation.of(new TypeHint<Map<String,String>>(){}));

        //OutputTag<WxChat> wxChatTag = new OutputTag<>("wxChat-state", PojoTypeInfo.of(WxChat.class));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.180.234:9092,192.168.180.235:9092,192.168.180.238:9092");
        properties.setProperty("zookeeper.connect", "192.168.180.234:2181,192.168.180.235:2181,192.168.180.238:2181");
        properties.setProperty("group.id", "wxChat");

        String topic = "test";
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        //kafkaConsumer.setStartFromGroupOffsets();
        kafkaConsumer.setStartFromLatest();

        SingleOutputStreamOperator<Map<String, String>> streamOperator = env.addSource(kafkaConsumer).uid("source_id")
                .map(new MapFunction<String, WxChat>() {
                    @Override
                    public WxChat map(String s) throws Exception {
                        JSONObject record = JSON.parseObject(s);

                        return new WxChat(
                                record.getString("wxId"),
                                record.getString("talker"),
                                record.getString("roomMsgWxId"),
                                record.getString("chatCreateTime"),
                                record.getString("type"));
                    }
                })
                .uid("map_id")
                .returns(TypeInformation.of(WxChat.class))
                // 延迟 5s
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<WxChat>(Time.seconds(5)) {
                    @SneakyThrows
                    @Override
                    public long extractTimestamp(WxChat wxChat) {
                        long timestamp = getCurrentWatermark().getTimestamp();
                        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String date = sd.format(new Date(timestamp));
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                        long time = simpleDateFormat.parse(wxChat.getChatCreateTime()).getTime();
                        println("==============> wxChat:" + wxChat + "; 时间：" + wxChat.getChatCreateTime()
                                + "; 获取到时间：" + time + "; 当前的 waterMark : " + timestamp + "; 当前的时间是： " + date);
                        return time;
                    }
                }).uid("wm_id")
                .keyBy(new KeySelector<WxChat, String>() {
                    @Override
                    public String getKey(WxChat wxChat) throws Exception {
                        return wxChat.getWxId() + "," + wxChat.getTalker() + "," + wxChat.getRoomMsgWxId();
                    }
                })
                /**
                 * 滚动窗口，1min, 10s trigger, 5s 延迟
                 * 第一次触发：
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:11"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:20"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:25"}
                 *
                 * 第一条数据是2020-05-11 17:11:11 10s触发，因此第一次 waterMark的 2020-05-11 17:11:20 由于可以延迟 5s.因此第一次真正触动的时间是 2020-05-11 17:11:25
                 *
                 * 第二次触发： 第二次触发的 waterMark是 2020-05-11 17:11:30 延迟 5S 第二次真正触发的时间是 2020-05-11 17:11:35
                 * {"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:30"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:34"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:35"}
                 *
                 * 一次类推，在 2020-05-11 17:11:00 到 020-05-11 17:12:00 waterMark的时间是 20/30/40/50 而 触发的时间是 25/35/45/55
                 *
                 * 当到达 020-05-11 17:12:00的时候，滚动窗口滚到到下一个 12分的窗口中，然而
                 * {"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:58"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:59"}
                 * 2020-05-11 17:11:55 到 2020-05-11 17:12：00 的数据还没有触发。
                 * 根据在11分的滚动窗口中计算可知，2020-05-11 17:11:00 到 2020-05-11 17:12:00之间的数据的
                 * waterMark 是 2020-05-11 17:12:00，
                 * 触发的时间是 2020-05-11 17:12:05. 因此当数据流中的数据到达
                 * {"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:00"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:01"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:05"}
                 * 12：05 的时候，触发计算 11分 最后五秒的数据：
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:58"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:59"}
                 *
                 * 然而 12分的数据不会参加11 分窗口的数据计算；
                 * 然后根绝12分 窗口的第一条数据来计算，12分窗口的第一个waterMark
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:12:00 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:12:01 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:12:05 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:12:09 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:12:10 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:12:12 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:12:15 CST 2020)
                 *
                 * 当前的waterMark：1589188330000 2020-05-11 17:12:10
                 *
                 * 可以看出，12分窗口的第一个 waterMark是 2020-05-11 17:12:10，延迟5S 因此第一次触发的时间是 2020-05-11 17:12:15
                 *
                 *
                 * 延迟数据：
                 *
                 *
                 */
                //.timeWindow(Time.minutes(1))
                // offset Time.hours(-8)
                /**
                 * 测试 offset 加上与不加的区别
                 */
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                // 允许数据延迟的时间是
                // 允许延迟的时间是 5s
                //.allowedLateness(Time.seconds(5))
                // 处理延迟数据
                /**
                 * Flink默认的时间是毫秒
                 *
                 * 处理迟到的数据： 迟到的数据是针对一整个的窗口来讲的
                 * 比如：滚动的窗口 1 min, 在一分钟以内每 10s触发一次计算。因此只要是在 11分到12分之间的所有的数据都是11分窗口里面的数据
                 * 只要eventTime < 12:00 的数据（eventTime在 11分 这一分钟的滚动窗口时间内）都不算迟到的数据。
                 * 只要当滚动窗口从11分 滚动到 12分的时候，这个时候的 waterMark >= 12:00的时候，窗口发生来滚动，
                 * 只要滚动到12分窗口时，11分窗口的最后一次还没有触发，
                 * 例如: 11分窗口的最后一次触发的时间是 12：05，
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:59"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:00"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:58"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:41"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:00"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:05"}
                 * 当输入 2020-05-11 17:12:05时，trigger 11分窗口的最后一次 waterMark
                 * 输出的数据：
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:59 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:58 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:41 CST 2020)
                 *
                 * 因此，这段时间无论来多少条 11分窗口的数据，都不算是延迟数据，
                 * 11 分窗口的最后一次都触发完成以后， 这个时候 数据的 eventTime 还是 11分窗口的数据，才算是延迟数据
                 *
                 * 例如：
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:05"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:55"}
                 *
                 * 12：05 以后再输入一条 11：55 的数据，
                 * 输出 打印：
                 * late data:> WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:55 CST 2020)
                 * 这个时候只要来 11 分的数据都算是延迟的数据。这些数据并不会统计在11分的窗口和12分的窗口中
                 * 第一： 可以直接丢弃
                 * 第二： 使用 .sideOutputLateData(wxChatTag) 使用侧数据流来输出迟到的数据。
                 *      也可以在 processFunction(Context context){
                 *          context.output();进行输出
                 *      }
                 *      使用 .sideOutputLateData(wxChatTag) 只会对延迟数据进行侧输出，并不会对原本窗口的数据产生影响
                 *      可以将延迟的数据输出来数据库中进行其他的处理
                 * 第三： .allowedLateness(Time.seconds(5)) 和 .sideOutputLateData(wxChatTag)
                 * 同时一起使用的效果： trigger （10S）,延迟 5S，整个窗口的数据 允许延迟 5S 因此有三个时间点，
                 * waterMark: 12：00
                 * trigger: 12:05
                 * allowedLateness: 12:10分的数据
                 * .allowedLateness(Time.seconds(5)) 数据可以延迟 5s，11分窗口的最后一次触发的时间是 12：05
                 * eventTime < 12:05的时候来的 11分的数据，这个部分的数据都是 11 分窗口的数据，不算延迟的数据
                 * 12:05 < eventTime < 12:10 的数据到来,这部分数据属于在.allowedLateness(Time.seconds(5))这个范围的数据
                 * 因此此时来一条 11 分窗口的数据，都会 trigger一下窗口计算一下数据。修改一下PV的值
                 *
                 * 当eventTime = 12：10 秒的数据到达以后，在来eventTime < 12:00 的数据，这部分数据都会从侧输出流输出
                 * late data:> WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:59 CST 2020)
                 *
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:11"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:55"}
                 *
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:11 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:55 CST 2020)
                 *
                 * result:  (wxid_besm9vc3zlju22,9547148812@chatroom,wxid_c8giongn8k7v22,2)
                 *
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:59"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:00"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:01"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:05"}
                 *
                 *
                 * 当前的waterMark：1589188320000 2020-05-11 17:12:00
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:59 CST 2020)
                 *
                 * result:  (wxid_besm9vc3zlju22,9547148812@chatroom,wxid_c8giongn8k7v22,3)
                 * 可以看出 11 分最后的一个waterMark是 2020-05-11 17:12:05，当时间来到 2020-05-11 17:12:05只触发了 11分的一条数据进行计算
                 *
                 *
                 * {"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:58"}
                 *
                 * 当前的waterMark：1589188320000 2020-05-11 17:12:00
                 * =============> WxChatStatisticsFunction is key:wxid_besm9vc3zlju22,9547148812@chatroom,wxid_c8giongn8k7v22
                 *  =========> pv的value 的值：3
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:58 CST 2020)
                 * ===========> 最后统计的结果key：wxid_besm9vc3zlju22,9547148812@chatroom,wxid_c8giongn8k7v22: value: 4<================
                 * result : (wxid_besm9vc3zlju22,9547148812@chatroom,wxid_c8giongn8k7v22,4)
                 *
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:30"}
                 *
                 * =============》 当前的waterMark：1589188320000 2020-05-11 17:12:00
                 * =============> WxChatStatisticsFunction is key:wxid_besm9vc3zlju22,9547148812@chatroom,wxid_c8giongn8k7v22
                 *  =========> pv的value 的值：4
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:30 CST 2020)
                 * ===========> 最后统计的结果key：wxid_besm9vc3zlju22,9547148812@chatroom,wxid_c8giongn8k7v22: value: 5<================
                 * (wxid_besm9vc3zlju22,9547148812@chatroom,wxid_c8giongn8k7v22,5)
                 *
                 *
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:09"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:31"}
                 *
                 * =============》 当前的waterMark：1589188324000
                 * =============> WxChatStatisticsFunction is key:wxid_besm9vc3zlju22,9547148812@chatroom,wxid_c8giongn8k7v22
                 *  =========> pv的value 的值：5
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:31 CST 2020)
                 * ===========> 最后统计的结果key：wxid_besm9vc3zlju22,9547148812@chatroom,wxid_c8giongn8k7v22: value: 6<================
                 * (wxid_besm9vc3zlju22,9547148812@chatroom,wxid_c8giongn8k7v22,6)
                 *
                 *
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:10"}
                 *
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:29"}
                 *
                 * ==============> 获取到时间：1589188289000
                 * =================> 获取key :WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:29 CST 2020)
                 * =================> 获取key :WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:29 CST 2020)
                 * late data:> WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:29 CST 2020)
                 *
                 * 在eventTime在 12：10分以后，再输入 2020-05-11 17:11:29的数据，此时会late data:延迟数据输出
                 *
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:12"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:15"}
                 *
                 * =============》 当前的waterMark：1589188330000 2020-05-11 17:12:10
                 *
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:12:00 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:12:01 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:12:05 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:12:09 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:12:12 CST 2020)
                 * =============> WxChatStatisticsFunction iterator:WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:12:15 CST 2020)
                 *
                 *
                 * result:(wxid_besm9vc3zlju22,9547148812@chatroom,wxid_c8giongn8k7v22,11)
                 *
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:59"}
                 *
                 * late data:> WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:59 CST 2020)
                 *
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:54"}
                 *
                 * late data:> WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:54 CST 2020)
                 *
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:20"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:29"}
                 *
                 * late data:> WxChat(wxId=wxid_besm9vc3zlju22, talker=9547148812@chatroom, roomMsgWxId=wxid_c8giongn8k7v22, chatCreateTime=Mon May 11 17:11:29 CST 2020)
                 *
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:12:25"}
                 *
                 *
                 */
                //.sideOutputLateData(wxChatTag)

                /**
                 *>{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:11"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:19"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:20"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:24"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:25"}
                 */
                // 每 10s触发一次 ContinuousEventTimeTrigger.of()
                // 延迟5S + 10s 触发，第一条数据是 2020-05-11 17:11:11
                // 因此第一次触发的时间是 2020-05-11 17:11:20 1589188280000
                // 因为第一条数据是 2020-05-11 17:11:11 那么第一次触发的时间是 2020-05-11 17:11:20
                // 但是因为 waterMark 延迟5s因此在触发的时间的后面再次延迟5s因此变成了 2020-05-11 17:11:25
                /**
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:27"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:29"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:30"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:34"}
                 * >{"wxId":"wxid_besm9vc3zlju22","talker":"9547148812@chatroom","roomMsgWxId":"wxid_c8giongn8k7v22","chatCreateTime":"2020-05-11 17:11:35"}
                 * 35的时候触发
                 */
                // 同理第二次触发，waterMark是 2020-05-11 17:11:30 1589188290000
                // 查看第二次触发的水印：1589188290000 2020-05-11 17:11:30
                // 因为 waterMark 延迟5s 因此第二次触发的时间还是 2020-05-11 17:11:35
                // 因此第一次和第二次时间相隔10s触发
                // 以此类推可以得到后续每次触发的数据
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                //.evictor(TimeEvictor.of(Time.seconds(0), true))
                .aggregate(new WxChatAggFunction(), new ResultWxChatFunction())
                //.process(new WxChatStatisticsFunction())
                .uid("agg_wxChat_ID")
                .process(new WxChatProcessFunction())
                .uid("process_wxChat_ID");


                streamOperator.print("dw: =====> ");
         streamOperator.addSink(new HBaseSinkFunction()).uid("wxChat_dwHBase_id");


        //streamOperator.getSideOutput(wxChatTag).print("late data:");

        env.execute("PV JOB");

    }

}
