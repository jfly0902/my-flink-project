package com.yskj.alink.task;

import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/27 11:11
 */
public class SentimentHotelSimpleExample {
    public static void main(String[] args) throws Exception {
        CsvSourceBatchOp source = new CsvSourceBatchOp().setFilePath("https://github.com/SophonPlus/ChineseNlpCorpus/raw/master/datasets"
                + "/ChnSentiCorp_htl_all/ChnSentiCorp_htl_all.csv")
                .setSchemaStr("label int, review string")
                .setIgnoreFirstLine(true);
        source.firstN(5).print();
    }
}
