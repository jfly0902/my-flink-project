package com.yskj.flink.util;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/28 9:53
 */
public class KafkaUtils {

    public static Properties buildKafkaProperties() {
        return buildKafkaProps(ParameterTool.fromSystemProperties());
    }

    public static Properties buildKafkaProps(ParameterTool parameterTool) {
        Properties properties = parameterTool.getProperties();
        properties.setProperty(PropertiesConstant.BOOTSTRAP_SERVERS, parameterTool.get(PropertiesConstant.BOOTSTRAP_SERVERS));
        properties.setProperty(PropertiesConstant.ZOOKEEPER_CONNECT, parameterTool.get(PropertiesConstant.ZOOKEEPER_CONNECT));
        properties.setProperty(PropertiesConstant.GROUP_ID, parameterTool.get(PropertiesConstant.GROUP_ID));
        properties.setProperty(PropertiesConstant.AUTO_OFFSET_RESET, parameterTool.get(PropertiesConstant.AUTO_OFFSET_RESET));
        properties.setProperty(PropertiesConstant.TOPIC, parameterTool.get(PropertiesConstant.TOPIC));
        return properties;
    }

}
