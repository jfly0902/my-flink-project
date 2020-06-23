package com.yskj.flink.window;

import com.yskj.flink.entity.UserTalkerEntity;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 *
 * @Author: jinxiang
 * @Date: 2020/4/20 16:22
 */
public class TopNHotTalker extends KeyedProcessFunction<Tuple, UserTalkerEntity, List<UserTalkerEntity>> {

    private int topSize;

    private ListState<UserTalkerEntity> itemState;

    public TopNHotTalker() {}

    public TopNHotTalker(int topSize) {
        this.topSize = topSize;
    }

    /**
     * 获取stat的
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        // 注册状态
        itemState = getRuntimeContext().getListState(new ListStateDescriptor<UserTalkerEntity>("listStat", UserTalkerEntity.class));

    }


    /**
     * 数据状态处理的逻辑
     * @param userTalkerEntity
     * @param context
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(UserTalkerEntity userTalkerEntity, Context context, Collector<List<UserTalkerEntity>> out) throws Exception {

        // 将数据全部加载进内存中
        itemState.add(userTalkerEntity);

        // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
        // 当时间达到的时候，会触发调用 onTimer()进行处理
        context.timerService().registerEventTimeTimer(userTalkerEntity.getWindowEnd() + 5 * 1000L);
    }

    /**
     * 当processElement的注册的时间达到时候会调用该方法处理
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<UserTalkerEntity>> out) throws Exception {
        // 将所有的数据取出，然后进行排序
        List<UserTalkerEntity> list = new ArrayList<>();

        for (UserTalkerEntity userTalkerEntity: itemState.get()) {
            list.add(userTalkerEntity);
        }

        //对list集合中的 实体类的属性 count进行排序 从大到小进行排列

        list.sort(new Comparator<UserTalkerEntity>() {
            @Override
            public int compare(UserTalkerEntity o1, UserTalkerEntity o2) {
                return (int) (o2.getCount() - o1.getCount());
            }
        });

        List<UserTalkerEntity> res = new ArrayList<>(topSize);

        list.subList(0,5).forEach(userTalkerEntity -> res.add(userTalkerEntity));

        // 清空内存状态
        itemState.clear();

        out.collect(res);
    }
}
