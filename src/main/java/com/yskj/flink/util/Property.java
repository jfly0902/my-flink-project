package com.yskj.flink.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/28 11:08
 */
@Slf4j
public class Property {

    private static final String CONF_NAME = "config.properties";

    private static final String CHARSET = "UTF-8";

    private static Properties properties;

    static {

        InputStream resource = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONF_NAME);
        properties = new Properties();

        try {
            InputStreamReader reader = new InputStreamReader(resource, CHARSET);
            properties.load(reader);
        } catch (UnsupportedEncodingException e) {
            log.info(" 编码: {}" + CHARSET + " 有误！");
        } catch (IOException e) {
            log.info(" =====> 资源加载失败 <=====");
        }

        log.info("=====> 资源加载完成 <=====");
    }

    public static String getValue(String key) {
        return properties.getProperty(key);
    }

    public static int getValueStr2Int(String key) {
        return StringUtils.isEmpty(getValue(key)) ? 0 : Integer.parseInt(getValue(key));
    }
}
