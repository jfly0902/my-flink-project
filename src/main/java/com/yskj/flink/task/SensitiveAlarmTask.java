package com.yskj.flink.task;

import cn.hutool.crypto.digest.MD5;
import cn.hutool.dfa.WordTree;
import com.alibaba.fastjson.JSON;
import com.yskj.flink.entity.SensitiveWord;
import com.yskj.flink.util.HBaseClientUtils;
import com.yskj.flink.util.HBaseTableColumn;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/8 14:04
 */
public class SensitiveAlarmTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*env.enableCheckpointing(18000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig config = env.getCheckpointConfig();
        config.setMaxConcurrentCheckpoints(3);
        config.setCheckpointTimeout(15 * 60 * 1000L);
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setMinPauseBetweenCheckpoints(500);
        config.setPreferCheckpointForRecovery(true);

        // 设置状态后端
        env.setStateBackend(new RocksDBStateBackend("hdfs://node1:9000/flink"));*/

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.180.234:9092,192.168.180.235:9092,192.168.180.238:9092");
        properties.setProperty("zookeeper.connect", "192.168.180.234:2181,192.168.180.235:2181,192.168.180.238:2181");
        properties.setProperty("group.id", "sensitive");

        String topic = "wxChat";


        // 用户数据
        FlinkKafkaConsumer<String> userConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        // userConsumer.setStartFromGroupOffsets();
        userConsumer.setStartFromLatest();

        // kafka消费的wxChat的聊天的数据
        SingleOutputStreamOperator<Map<String, String>> userStream = env.addSource(userConsumer)
                .uid("wxChat_user_source_id")
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        System.out.println("=======kafka 的数据 ========> :" + s);
                        return StringUtils.isNotEmpty(s);
                    }
                })
                .map(new MapFunction<String, Map<String, String>>() {
                    @Override
                    public Map<String, String> map(String json) throws Exception {
                        Map<String, String> jsonMap = (Map) JSON.parse(json);
                        Map<String, String> resultMap = new HashMap<>();
                        resultMap.put("wxId", jsonMap.get("wxId"));
                        resultMap.put("talker", jsonMap.get("talker"));
                        resultMap.put("roomMsgWxId", jsonMap.get("roomMsgWxId"));
                        resultMap.put("chatCreateTime", jsonMap.get("chatCreateTime"));
                        resultMap.put("content", jsonMap.get("content"));
                        resultMap.put("isSend", jsonMap.get("isSend"));
                        return resultMap;
                    }
                }).uid("source_json_map_ID");


        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "192.168.180.234:9092,192.168.180.235:9092,192.168.180.238:9092");
        properties1.setProperty("zookeeper.connect", "192.168.180.234:2181,192.168.180.235:2181,192.168.180.238:2181");
        properties1.setProperty("group.id", "dim");

        String topic1 = "dim";

        // 维表的数据
        FlinkKafkaConsumer<String> sensitiveWordConsumer = new FlinkKafkaConsumer<>(topic1, new SimpleStringSchema(), properties1);
        sensitiveWordConsumer.setStartFromLatest();

        // 敏感词的流，将该流进行广播
        SingleOutputStreamOperator<SensitiveWord> sensitiveStream = env.addSource(sensitiveWordConsumer)
                .uid("sensitive_word_source_id").filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        System.out.println("===========> 维度数据：" + s);
                        return StringUtils.isNotEmpty(s);
                    }
                })
                .map(s -> JSON.parseObject(s, SensitiveWord.class))
                .uid("sensitive_json2SensitiveWord_Id");

        // mapState 存储 维表的信息
        MapStateDescriptor<String, String> sensitiveWordState = new MapStateDescriptor<>("sensitive_word_state",
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        String topic2 = "sen";
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(topic2, new SimpleStringSchema(), properties);

        // 两个流进行join , 将维表流进行广播
        userStream.connect(sensitiveStream.broadcast(sensitiveWordState))
                .process(new BroadcastProcessFunction<Map<String, String>, SensitiveWord, String>() {
                    private WordTree wordTree;
                    private HashMap<String, String> sensitiveHashMap;

                    @Override
                    public void processElement(Map<String, String> map, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                        if (!map.isEmpty()) {
                            List<String> list = matchSensitiveWord(wordTree, map.get("content"));
                            System.out.println("匹配到的敏感词的数据： " + list.toString());
                            if (!CollectionUtils.isEmpty(list)) {
                                // 将敏感数据存入HBASE数据库中。
                                // writeHBaseSensitive(map);
                                collector.collect(JSON.toJSONString(map));
                            }
                        }

                    }

                    // 更新维表中的敏感词
                    // 首先会使用该方法获取广播的stream的数据
                    @Override
                    public void processBroadcastElement(SensitiveWord sensitiveWord, Context context, Collector<String> collector) throws Exception {
                        if (wordTree == null) {
                            wordTree = new WordTree();
                        }
                        if (sensitiveHashMap == null) {
                            sensitiveHashMap = new HashMap<>();
                        }
                        BroadcastState<String, String> broadcastState = context.getBroadcastState(sensitiveWordState);
                        if (!sensitiveWord.isStatus()) {
                            broadcastState.remove(sensitiveWord.getId());
                            sensitiveHashMap.remove(sensitiveWord.getId());
                            wordTree.clear();
                        } else {
                            broadcastState.put(sensitiveWord.getId(), sensitiveWord.getWord());
                            sensitiveHashMap.put(sensitiveWord.getId(), sensitiveWord.getWord());
                            wordTree.clear();
                        }
                        wordTree.addWords(sensitiveHashMap.values());
                        System.out.println("构建的 wordTree的数据：" + wordTree.toString());
                    }

                    /**
                     * 敏感词匹配
                     * @return
                     */
                    private List<String> matchSensitiveWord (WordTree wordTree, String word) {
                        /**
                         * matchAll方法，WordTree还提供了match和isMatch两个方法，
                         * 这两个方法只会查找第一个匹配的结果，
                         * 这样一旦找到第一个关键字，就会停止继续匹配，大大提高了匹配效率。
                         */
                        List<String> matchList = new ArrayList<>();
                        if(wordTree == null || StringUtils.isEmpty(word)) {
                            return matchList;
                        }
                        return wordTree.matchAll(word, -1, true, true);
                    }

                    /**
                     * 将具有敏感词的用户存入数据库中
                     * @param map
                     */
                    private void writeHBaseSensitive(Map<String, String> map) throws IOException {
                        String rowKey = getRowKey(map);
                        HBaseClientUtils.writeRecord(HBaseTableColumn.SEN_WXCHAT_TABLE, rowKey, HBaseTableColumn.SEN_WXCHAT_FAMILY, map);
                    }

                    /**
                     * 获取rowKey
                     * @param map
                     * @return
                     */
                    private String getRowKey(Map<String, String> map) {
                        StringBuilder rowKey = new StringBuilder();
                        byte[] digest = MD5.create().digest(map.get("wxId") + map.get("talker") +
                                map.get("roomMsgWxId") + map.get("chatCreateTime"));
                        int i = Arrays.hashCode(digest) % 3;
                        int abs = Math.abs(i);
                        String before = "00" +abs + "_";
                        String dateString = getDateString();
                        return rowKey.append(before).append(dateString).append("_").append(digest.toString()).toString();
                    }

                    /**
                     * 获取时间
                     * @return
                     */
                    private String getDateString() {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
                        return simpleDateFormat.format(new Date());
                    }
                }).uid("broadcast_process_Id")
                // 将处理后的数据写入kafka中，提供给业务使用，发出警告等行为
                .addSink(kafkaProducer);

        env.execute(" WxChat sensitive Job");
    }
}
