package com.yskj.flink.function;

import cn.hutool.crypto.digest.MD5;
import com.yskj.flink.entity.DwsMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: xiang.jin
 * @Date: 2020/7/8 16:35
 */
@Slf4j
public class DataSkewAggProcessFunction extends KeyedProcessFunction<String, DwsMsg, DwsMsg> {

    //private MapState<String, DwsMsg> mapState;
    private ListState<DwsMsg> listState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();
         /*MapStateDescriptor<String, DwsMsg> descriptor = new MapStateDescriptor<>("state_data_skew_sum",
                TypeInformation.of(String.class), TypeInformation.of(DwsMsg.class));*/
        ListStateDescriptor<DwsMsg> descriptor = new ListStateDescriptor<>("state_data_skew_sum", DwsMsg.class);
        //descriptor.enableTimeToLive(ttlConfig);
        //mapState = this.getRuntimeContext().getMapState(descriptor);
        listState = this.getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(DwsMsg input, Context context, Collector<DwsMsg> out) throws Exception {
        listState.add(input);
        context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DwsMsg> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        Map<String, DwsMsg> result = new HashMap<>(10000);
        for (DwsMsg dwsMsg : listState.get()) {
            String key = dwsMsg.getTalker() + "," + dwsMsg.getRoomMsgWxId() + ","
                                    + dwsMsg.getWeChatTime() + "," + dwsMsg.getWindowEnd();
            if (!result.containsKey(key)) {
                result.put(key, dwsMsg);
            } else {
                DwsMsg msg = result.get(key);
                msg.setPvValue(String.valueOf(Integer.parseInt(msg.getPvValue()) + Integer.parseInt(dwsMsg.getPvValue())));
            }
        }
        result.values().forEach(dwsMsg -> {
                out.collect(dwsMsg);
        });
        listState.clear();
    }
}
