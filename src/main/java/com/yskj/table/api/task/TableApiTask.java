package com.yskj.table.api.task;

import com.sun.prism.PixelFormat;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Schema;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/18 15:01
 */
public class TableApiTask {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> streamSource = env.readTextFile("");

        Table order = tEnv.from("order");

        /** select * from order where id = 1 group BY id, name */
        Table select = order.filter("id === 1").groupBy("id,name").select("");

        // Or

        Table table = tEnv.sqlQuery(
                "select * from order" +
                        "Where id =1" +
                        "group BY id,name"
        );


        // 注册一个TABLE sinkTable

        Schema schema = new Schema();
        schema.field("id", DataTypes.INT())
                .field("name", DataTypes.VARCHAR(512))
                .field("age", DataTypes.BIGINT());

        tEnv.connect(new FileSystem().path(""))
                .withFormat(new Avro())
                .withSchema(schema)
                .createTemporaryTable("csvSinkTable");

        Table result = tEnv.sqlQuery("");

        result.insertInto("csvSinkTable");


        // dataStream 转换成table
        Table tableStream = tEnv.fromDataStream(streamSource);

        // Convert the DataStream into a Table with fields "myLong", "myString"
        Table table2 = tEnv.fromDataStream(streamSource, "myLong, myString");

    }

}
