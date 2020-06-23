package com.yskj.flink.task;

import com.yskj.flink.entity.UserBehavior;
import com.yskj.flink.util.UserBehaviorAvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/22 17:27
 */
public class KafkaAvroProduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        // 开启checkpoint 3分钟check一次，check的模型是 EXACTLY_ONCE
        env.enableCheckpointing(180000, CheckpointingMode.EXACTLY_ONCE);
        // 程序挂掉以后，checkpoint可以保存到外部的文件中， check清理模式 CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // check的超时设置， 默认是10分钟，可根据实际业务state 的大小设置
        env.getCheckpointConfig().setCheckpointTimeout(15 * 60 * 1000L);
        // check 的时候允许的最大并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // check 失败以后间隔多久重试
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 当存在较新的保存点时，允许作业恢复回退到检查点
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        // 开启Avro的序列化机制
        env.getConfig().enableForceAvro();

        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "192.168.180.239:2181");

        String topic = "test01";
        //FlinkKafkaProducer<UserBehavior> kafkaProducer = new FlinkKafkaProducer<UserBehavior>(topic,  new UserBehaviorAvroSchema(topic), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        AvroDeserializationSchema<UserBehavior> specific = AvroDeserializationSchema.forSpecific(UserBehavior.class);
        DataStreamSource<UserBehavior> specificSource = env.addSource(new FlinkKafkaConsumer<>(topic, specific, properties));
        Schema schema = Schema.parse("{\"namespace\": \"com.example.flink.avro\",\"type\": \"record\",\"name\": \"UserInfo\",\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}");
        AvroDeserializationSchema<GenericRecord> generic = AvroDeserializationSchema.forGeneric(schema);
        DataStreamSource<GenericRecord> streamSource = env.addSource(new FlinkKafkaConsumer<>(topic, generic, properties).setStartFromEarliest());



        env.execute("kafka Avro");

    }
}
