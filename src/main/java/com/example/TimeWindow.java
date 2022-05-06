package com.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 时间窗口
 * 统计每1分钟用户购买的商品数量
 */
public class TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<OrderData> map = textStream.map(new MapFunction<String, OrderData>() {
            @Override
            public OrderData map(String s) throws Exception {
                //1,1
                //1,1
                String[] arr = s.split(",");
                return new OrderData(Integer.valueOf(arr[0]), Integer.valueOf(arr[1]));
            }
        });
        //分组
        KeyedStream<OrderData, Integer> keyedStream = map.keyBy(OrderData::getUserId);
        //1、滚动时间窗口，无重叠，窗口大小1分钟
//        SingleOutputStreamOperator<OrderData> count1 = keyedStream.window(TumblingProcessingTimeWindows.of(Time.minutes(1))).sum("buyCnt");
//        count1.print();

        //2、滑动时间窗口，有重叠，每30秒计算一次最近1分钟用户购买的商品数量
        SingleOutputStreamOperator<OrderData> count2 = keyedStream.window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30))).sum("buyCnt");
        count2.print();

        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderData{
        private Integer userId;
        private Integer buyCnt;
    }
}
