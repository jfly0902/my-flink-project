package com.yskj.flink.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/24 17:45
 */
public class UvBloomFunction extends ProcessWindowFunction<String, String, Tuple, TimeWindow> {

        Jedis jedis = new Jedis();
        BloomFilter bloomFilter = new BloomFilter(1 << 29);

    @Override
    public void process(Tuple key, Context context, Iterable<String> iterable, Collector<String> out) throws Exception {

        // 存储在redis的key
        String storeKey = String.valueOf(context.window().getEnd());

        long count = 0L;

        // 把每个窗口的uv值存到 redis中的count表中，内容为 （windowEnd -> uvCount）

        String s = jedis.hget("count", storeKey);
        if (null != s) {
            count = Long.parseLong(s);
        }

        // 用bloom 过滤器判断这个用户是否存在
        String userId = key.getField(0).toString().substring(6, key.getField(0).toString().length());

        long offset = bloomFilter.bloomHash(userId, 61);
        
        // 定义一个标示位，判断改位置是否存在值
        Boolean isExist = jedis.getbit(storeKey, offset);

        if (!isExist) {
            jedis.setbit(storeKey, offset, true);
            count += 1;
            jedis.hset("count", storeKey, String.valueOf(count));
            out.collect(key.getField(0).toString() + " : " + count);
        } else {
            out.collect(key.getField(0).toString() + " : " + count);
        }

    }
}
