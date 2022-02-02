package com.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 消费kafka
 * sink到kafka
 */
public class SinkKafkaDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","flink");//消费组id
        // latest
        // earliest
        properties.setProperty("auto.offset.reset","latest");
        //每5秒检测kafka分区情况
        properties.setProperty("flink.partition-discovery-interval-millis","5000");
        properties.setProperty("enable.auto.commit","true");//自动提交
        properties.setProperty("auto.commit.interval.ms","2000");//自动提交时间间隔
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("my-topic1", new SimpleStringSchema(),properties));
        SingleOutputStreamOperator<String> success = dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.contains("success");
            }
        });
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","localhost:9092");
        success.addSink(new FlinkKafkaProducer<String>("my-topic2",new SimpleStringSchema(),prop));
        env.execute();
    }
}
