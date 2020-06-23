package com.yskj.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author: xiang.jin
 * @Date: 2020/4/28 15:51
 */
public class CustomSourceFunction implements SourceFunction<String> {

    /**
     * 启动源，
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        // 数据的封装操作，将一条数据封装好以后使用
        // 使用 SourceContext#collect()将数据进行输出
        sourceContext.collect("");

    }

    /** 停止发送 */
    @Override
    public void cancel() {

    }
}
