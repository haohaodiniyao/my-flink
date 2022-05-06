package com.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 统计一分钟用户购买商品数量
 */
public class WindowDemo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Order> map = textStream.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String s) throws Exception {
                String[] arr = s.split(" ");
                return new Order(Integer.valueOf(arr[0]), Integer.valueOf(arr[1]));
            }
        });
        //分组
        KeyedStream<Order, Integer> keyedStream = map.keyBy(Order::getUserId);

        //1、session超时时间10秒，10秒内没有数据到来，触发上个窗口计算
        SingleOutputStreamOperator<Order> count = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).sum("count");
        count.print();

        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order{
        private Integer userId;//用户id
        private Integer buyCnt;//购买商品数量
    }
}
