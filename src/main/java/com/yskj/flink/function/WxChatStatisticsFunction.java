package com.yskj.flink.function;

import com.yskj.flink.entity.WxChat;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Util.println;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/11 10:25
 */
public class WxChatStatisticsFunction extends ProcessWindowFunction<WxChat, Map<String,String>, String, TimeWindow> {

    // 统计key 的 pv
    private transient ValueState<Integer> pvValueState;

    /**
     * 第一次触发的时候，pv原始值为0，计算完成以后pv的值为5
     * 第二次触发是， PV由于第一次的时候存在值，因此原始的值为5，
     * 而第二次触发的时候，由于之前Iterable<WxChat> input已经保存了窗口的全部的值所以，将前一个waterMark的值也计算其中，
     * 因此第二次触发的时候算的pv 为10，由于之前pv的值为5，因此相加以后为15。显然这个15的值不正确，正确的值就是10，
     * 因此要把第一次触发的时候的值给去掉。
     *
     * 在使用.evictor(TimeEvictor.of(Time.seconds(0), true))
     * 保存最近0秒的数据，
     * 第一次触发时，pv的原始数据还是0，第一次到 trigger的时候触发时的 pv=6
     * 第二次再输入4条数据到触发的条件的时候，原始的pv =6,
     * 使用了.evictor(TimeEvictor.of(Time.seconds(0), true))
     * 过滤出最近的0s的数据，因此在第一次触发的数据全部被过滤掉了，
     * 这样只计算第二次触发的时候，第一次和第二次之间的数据 4条，因此pv就是 6+4 = 10
     *
     *
     * 当 .evictor(TimeEvictor.of(Time.seconds(1), true))
     * 保存最近一秒钟的数据，也就是保存第一个触发后的一秒的数据，
     * 也就是保存 2020-05-11 17:11:24 < time <= 2020-05-11 17:11:25 之间1s的数据的条数
     *
     * 设置过期时间：ttl ttl设置的是3分钟,是对状态后端pvValueState的值进行过滤。
     *
     *
     *
     * @param parameters
     * @throws Exception
     */

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.minutes(3))
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();

        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("pv-state", Integer.class);
        valueStateDescriptor.enableTimeToLive(ttlConfig);
        pvValueState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void process(String key, Context context, Iterable<WxChat> input, Collector<Map<String,String>> out) throws Exception {

        // 获取当前pv的value值
        Integer value = pvValueState.value();

        if (value == null) {
            value = 0;
        }
        println(" =========> pv的value 的值：" + value);
        Iterator<WxChat> iterator = input.iterator();
        while (iterator.hasNext()) {
            WxChat wxChat = iterator.next();
            println("=============> WxChatStatisticsFunction iterator:" + wxChat);
            value += 1;
        }
        pvValueState.update(value);
        /**
         * 对侧输出的数据进行输出
         */
        //context.output();
        TimeWindow window = context.window();

        long l = context.currentWatermark();

        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = sd.format(new Date(l));

        Map<String, String> map = new HashMap<>();
        map.put(key, value.toString());
        println("===========> 最后统计的结果key：" + key + ": value: " + value + "<================" +
                " start window : " + window.getStart() +  "; end window : " + window.getEnd() + "; 当前的WM: " + l + "; 当前时间： " + date);
        out.collect(map);
    }
}
