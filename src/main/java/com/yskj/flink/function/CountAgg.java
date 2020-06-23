package com.yskj.flink.function;

import com.yskj.flink.entity.TalkerEntity;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Author: jinxiang
 * @Date: 2020/4/20 14:52
 */
/** 返回类型 */

/**
 * TalkerEntity 聚合的值的类型（输入值）
 * <ACC> 累加器的类型（中间聚合状态）。
 * OUT 聚合结果的类型
 */
public class CountAgg implements AggregateFunction<TalkerEntity, Long, Long> {
    /**
     * 初始状态的值
     * 创建一个累加器，初始化累加器
     * @return
     */
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    /**
     * 累加的逻辑，将给定的输入值添加到给定的累加器，返回新的累加器值
     *
     * @param talkerEntity 要增加的值，这块跟talkerEntity 没有关系
     * @param aLong 要将值添加到的累加器
     * @return 具有更新状态的累加器
     */
    @Override
    public Long add(TalkerEntity talkerEntity, Long aLong) {
        return aLong + 1;
    }

    /**
     * 输出的结果
     * @param aLong  累加器
     * @return 返回最终的结果
     */
    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    /**
     * 累加器值得处理
     * @param aLong 一个累加器
     * @param acc1 另外一个累加器
     * @return 两个累加器合并的状态
     */
    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}
