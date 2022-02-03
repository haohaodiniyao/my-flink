package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/table/common.html
 */
public class TableDemo {
    public static void main(String[] args) throws Exception {
        //1、env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //2、source
        DataStreamSource<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "a", 1),
                new Order(2L, "b", 2),
                new Order(1L, "a", 1)
        ));
        DataStreamSource<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(4L, "d", 1),
                new Order(5L, "e", 2),
                new Order(1L, "c", 3)
        ));

        Table tableA = tableEnv.fromDataStream(orderA, $("userId"), $("product"), $("amount"));
        tableEnv.createTemporaryView("tableB", orderB, $("userId"), $("product"), $("amount"));

        Table tableResult = tableEnv.sqlQuery("select * from " + tableA);

//        tableEnv.toAppendStream(tableResult,Order.class).print();
//        toRetractStream
        DataStream<Tuple2<Boolean, Order>> tuple2DataStream = tableEnv.toRetractStream(tableResult, Order.class);
        tuple2DataStream.print();

        env.execute();

    }
}
