package com.yskj.flink.function;

import com.yskj.flink.entity.ItemViewCount;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/21 14:08
 */
/** 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串 */
public class TopNItemsFunction extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

    private final int topSize;

    public TopNItemsFunction(int topSize) {
        this.topSize = topSize;
    }

    // 保证在发生故障时，状态数据的不丢失和一致性。
    // ListState 是 Flink 提供的类似 Java List 接口的 State API，
    // 它集成了框架的 checkpoint 机制，自动做到了 exactly-once 的语义保证。
    private ListState<ItemViewCount> itemListState;

    //private MapState<String, Map<String, Long>> mapState;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 设置TTl
        // 目前TTL只适用于 processTime
        /*StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.minutes(5))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .cleanupInRocksdbCompactFilter(180000)
                .build();*/


        // 状态的注册
        ListStateDescriptor<ItemViewCount> stateDescriptor = new ListStateDescriptor<>("item-state", ItemViewCount.class);

        // stateDescriptor.enableTimeToLive(ttlConfig);

        /*MapStateDescriptor<String, Map<String, Long>> mapStateDescriptor = new MapStateDescriptor<>("map-state",
                BasicTypeInfo.STRING_TYPE_INFO, new MapTypeInfo<>(String.class, Long.class));
        mapState = getRuntimeContext().getMapState(mapStateDescriptor);*/

        itemListState = getRuntimeContext().getListState(stateDescriptor);
    }

    @Override
    public void processElement(ItemViewCount input, Context context, Collector<String> out) throws Exception {

        // 保存窗口中过来的每一条数据
        itemListState.add(input);
        // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
        // 触发 onTime()
        context.timerService().registerEventTimeTimer(input.windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        // 获取得到的所有的商品点击量
        List<ItemViewCount> allItemList = new ArrayList<>();
        itemListState.get().forEach(allItemList::add);
        // itemListState 中的数据已经全部 给到了 allItemList 可以释放 itemListState 的空间
        itemListState.clear();

        // 对 allItemList进行排序 从大到小
        allItemList.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int) (o2.viewCount - o1.viewCount);
            }
        });

        // 将排名信息格式化成 String, 便于打印
        StringBuilder result = new StringBuilder();
        result.append("====================================\n");
        result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
        for (int i=0;i<topSize;i++) {
            ItemViewCount currentItem = allItemList.get(i);
            // No1:  商品ID=12224  浏览量=2413
            result.append("No").append(i).append(":")
                    .append("  商品ID=").append(currentItem.itemId)
                    .append("  浏览量=").append(currentItem.viewCount)
                    .append("\n");
        }
        result.append("====================================\n\n");

        out.collect(result.toString());

    }
}
