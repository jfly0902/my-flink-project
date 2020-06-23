package com.yskj.flink.function;

import com.google.common.hash.Funnels;
import com.yskj.flink.entity.WxChat;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.google.common.hash.BloomFilter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/6 18:54
 */
public class ReduceWxChatFunction extends ProcessWindowFunction<Map<String, String>, Map<String, Integer>, String, TimeWindow> {

    // private transient ValueState<BloomFilter<String>> bloomFilterState;
    // private transient ValueState<Integer> uvState;
    private transient ValueState<Integer> pvState;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 设置ttl
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.minutes(30))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
        //ValueStateDescriptor<BloomFilter<String>> bloomDescriptor = new ValueStateDescriptor<BloomFilter<String>>("bloom", TypeInformation.of(new TypeHint<BloomFilter<String>>(){}));
        //ValueStateDescriptor<Integer> uvStateDescriptor = new ValueStateDescriptor<>("uv-state", Integer.class);
        ValueStateDescriptor<Integer> pvStateDescriptor = new ValueStateDescriptor<>("pv-state", Integer.class);

        //bloomDescriptor.enableTimeToLive(ttlConfig);
        //uvStateDescriptor.enableTimeToLive(ttlConfig);
        pvStateDescriptor.enableTimeToLive(ttlConfig);

        //bloomFilterState = getRuntimeContext().getState(bloomDescriptor);
        //uvState = getRuntimeContext().getState(uvStateDescriptor);
        pvState = getRuntimeContext().getState(pvStateDescriptor);
    }

    @Override
    public void process(String key, Context context, Iterable<Map<String, String>> element, Collector<Map<String, Integer>> collector) throws Exception {
        // BloomFilter<String> bloomFilter = bloomFilterState.value();
        //Integer uvValue = uvState.value();
        Integer pvValue = pvState.value();

        /*if (bloomFilter == null) {
            // 初始化数据
            //bloomFilter = BloomFilter.create(Funnels.stringFunnel(), 10 * 1000 * 1000);
            uvValue = 0;
            pvValue = 0;
        }*/

        Iterator<Map<String, String>> iterator = element.iterator();
        while (iterator.hasNext()) {
            pvValue += 1;
            /*Map<String, String> map = iterator.next();
            if (!bloomFilter.mightContain(key)) {
                bloomFilter.put(key);
                uvValue += 1;
            }*/
        }

        // bloomFilterState.update(bloomFilter);
        // uvState.update(uvValue);
        pvState.update(pvValue);

        Map<String, Integer> result = new HashMap<>();
        result.put(key, pvValue);
        collector.collect(result);
    }
}
