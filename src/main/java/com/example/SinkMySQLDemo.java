package com.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Date;

/**
 * sink数据到MySQL
 */
public class SinkMySQLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(new User(null,"wangwu",new Date(new java.util.Date().getTime())))
        .addSink(JdbcSink.sink(
                "insert into user (user_name, create_time) values (?,?)",
                (ps, t) -> {
                    ps.setString(1, t.userName);
                    ps.setDate(2, t.createTime);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:55007/mydb?useSSL=false")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()));
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
}
