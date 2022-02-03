package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/table/common.html
 */
public class FlinkSQLKafkaDemo {
    public static void main(String[] args) throws Exception {
        //1、env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.executeSql("create table input_kafka (\n" +
                " `user_id` bigint,\n" +
                " `status` string\n" +
                ") with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'input_kafka',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'format' = 'json'\n" +
                ")");
        Table table = tableEnv.sqlQuery("select * from input_kafka where status='success'");
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(table, Row.class);
        tuple2DataStream.print();
        tableEnv.executeSql("create table output_kafka (\n" +
                " `user_id` bigint,\n" +
                " `status` string\n" +
                ") with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'output_kafka',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'format' = 'json',\n" +
                " 'sink.partitioner' = 'round-robin'\n" +
                ")");

        tableEnv.executeSql("insert into output_kafka select * from input_kafka");
//不用写，否则报错
//        env.execute();

    }
}
