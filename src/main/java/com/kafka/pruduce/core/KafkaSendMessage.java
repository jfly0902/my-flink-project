package com.kafka.pruduce.core;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/23 16:20
 */
public class KafkaSendMessage {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "192.168.180.239:2181");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String topic = "test01";

        KafkaProducer producer = new KafkaProducer<>(properties);

        String msg = new String();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, null, null, msg);

        Future future = producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                long offset = recordMetadata.offset();
                int partition = recordMetadata.partition();
                recordMetadata.partition();
            }
        });

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));

    }

}
