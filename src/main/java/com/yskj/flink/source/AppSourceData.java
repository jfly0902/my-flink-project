package com.yskj.flink.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/23 17:55
 */
public class AppSourceData extends RichSourceFunction<String> {

    private volatile boolean running = true;

    private final String[] behaviorArr = {"click", "download", "install", "uninstall"};

    private final String[] channelArr = {"baidu", "UC", "weChat", "appStore"};

    Random random = new Random();

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        final long maxElement = 10000;

        long count = 0;

        while (running && count <= maxElement) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(UUID.randomUUID() + ",")
                    .append(behaviorArr[random.nextInt(behaviorArr.length-1)] + ",")
                    .append(channelArr[random.nextInt(channelArr.length-1)] + ",")
                    .append(System.currentTimeMillis());
            count++;
            sourceContext.collect(stringBuilder.toString());
            TimeUnit.MILLISECONDS.sleep(2L);
        }
        running = false;
    }

    @Override
    public void cancel() {
        running = false;
    }
}
