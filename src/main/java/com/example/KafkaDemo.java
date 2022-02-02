package com.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host","localhost");
        int port = parameterTool.getInt("port",9092);
        String output = parameterTool.get("output","/Users/yaokai/Downloads/output");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",host+":"+port);
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("my_topic", new SimpleStringSchema(),properties));
        dataStream.writeAsText(output);
        env.execute();
    }
}
