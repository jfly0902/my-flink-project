package com.yskj.flink.function;

import com.yskj.flink.entity.AppUserBehavior;
import com.yskj.flink.entity.AppViewCount;
import com.yskj.flink.entity.ItemViewCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/23 18:59
 */
public class AppMarketingWindowFunction extends ProcessWindowFunction<AppUserBehavior, String, String, TimeWindow> {


    @Override
    public void process(String key, Context context, Iterable<AppUserBehavior> input, Collector<String> out) throws Exception {

        String start = String.valueOf(context.window().getStart());
        String end = String.valueOf(context.window().getEnd());
        String channel = key.split(",")[0];
        String behavior = key.split(",")[1];
        AtomicInteger count = new AtomicInteger();
        input.forEach(appUserBehavior -> {
            count.getAndIncrement();
        });

        StringBuilder result = new StringBuilder();
        result.append("====================================\n");
        result.append("开始时间: ").append(start).append("\n");
        result.append("   渠道：").append(channel)
                    .append("  行为=").append(behavior)
                    .append("  总量=").append(count).append("\n")
                    .append(" 结束时间：").append(end)
                    .append("\n");
        result.append("====================================\n\n");

        out.collect(result.toString());
    }


}
