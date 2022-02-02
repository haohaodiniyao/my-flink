package com.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 从MySQL查询数据
 */
public class SourceMySQLDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStream<User> dataStream = env.addSource(new MySQLDataSource()).setParallelism(1);
//        dataStream.writeAsText("/Users/yaokai/Downloads/output", FileSystem.WriteMode.OVERWRITE);
        dataStream.print();
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class User{
        private Long id;
        private String userName;
        private Date createTime;
    }

    public static class MySQLDataSource extends RichParallelSourceFunction<User>{
        private boolean flag = true;
        private Connection connection = null;
        private PreparedStatement preparedStatement = null;
        private ResultSet resultSet = null;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:55007/mydb?useSSL=false", "root", "123456");
            String sql = "select id,user_name as userName,create_time as createTime from user";
            preparedStatement = connection.prepareStatement(sql);
        }

        @Override
        public void run(SourceContext<User> sourceContext) throws Exception {
            while (flag){
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()){
                    Long id = resultSet.getLong("id");
                    String userName = resultSet.getString("userName");
                    java.sql.Date createTime = resultSet.getDate("createTime");
                    sourceContext.collect(new User(id,userName,createTime));
                }
                Thread.sleep(5*60*1000);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }

        @Override
        public void close() throws Exception {
            if(connection != null) connection.close();
            if(preparedStatement != null) preparedStatement.close();

        }
    }
}
