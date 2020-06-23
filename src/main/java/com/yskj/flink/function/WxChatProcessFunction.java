package com.yskj.flink.function;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/14 10:55
 */
public class WxChatProcessFunction extends ProcessFunction<Map<String, String>, Map<String, String>> {

   // private ListState<Map<String, String>> listState;

    //private ValueState<Integer> pvState;


    /*@Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
        //ListStateDescriptor<Map<String, String>> descriptor = new ListStateDescriptor<>("list-state",
                //TypeInformation.of(new TypeHint<Map<String, String>>() {}));
        //descriptor.enableTimeToLive(ttlConfig);
        //listState = getRuntimeContext().getListState(descriptor);
        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("pv-state", BasicTypeInfo.INT_TYPE_INFO);
        descriptor.enableTimeToLive(ttlConfig);
        pvState = getRuntimeContext().getState(descriptor);
    }*/

    @Override
    public void processElement(Map<String, String> input, Context context, Collector<Map<String, String>> out) throws Exception {
        long watermark = context.timerService().currentWatermark();
        input.keySet().forEach(key -> System.out.println("=====> key :" + key + " , " + "====> value : " + input.get(key) + " 当前的waterMark: " + watermark));
        out.collect(input);
    }

    /*@Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Map<String, String>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        *//*for (Map<String, String> map : listState.get()){
            map.keySet().forEach(key -> System.out.println("=====> key :" + key + " , " + "====> value : " + map.get(key)));
            out.collect(map);
        }

        listState.clear();*//*

        Integer value = pvState.value();
        System.out.println("====> value : " + value);
        pvState.clear();
    }*/
}
