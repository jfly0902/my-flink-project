package com.yskj.flink.function;

import com.yskj.flink.acc.PvWxChatAcc;
import com.yskj.flink.entity.WxChat;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/14 10:16
 */

/**
 * AggregateFunction<IN, ACC, OUT>
 *     IN : 输入类型
 *     ACC： Accumulator
 *     OUT : 输出
 *
 */
public class WxChatAggFunction implements AggregateFunction<WxChat, PvWxChatAcc, Long> {

    private static final long serialVersionUID = 1L;


    @Override
    public PvWxChatAcc createAccumulator() {
        return new PvWxChatAcc();
    }

    @Override
    public PvWxChatAcc add(WxChat wxChat, PvWxChatAcc pvWxChatAcc) {
        pvWxChatAcc.key = wxChat.f0 + "," + wxChat.f1 + "," + wxChat.f2;
        pvWxChatAcc.pv += 1;
        return pvWxChatAcc;
    }

    @Override
    public Long getResult(PvWxChatAcc pvWxChatAcc) {
        return pvWxChatAcc.pv;
    }

    @Override
    public PvWxChatAcc merge(PvWxChatAcc pvWxChatAcc, PvWxChatAcc acc1) {
        pvWxChatAcc.pv += acc1.pv;
        return pvWxChatAcc;
    }
}
