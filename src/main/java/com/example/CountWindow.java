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
 * 计数窗口
 */
public class CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<OrderData> map = textStream.map(new MapFunction<String, OrderData>() {
            @Override
            public OrderData map(String s) throws Exception {
                String[] arr = s.split(",");
                return new OrderData(Integer.valueOf(arr[0]), Integer.valueOf(arr[1]));
            }
        });
        //分组
        KeyedStream<OrderData, Integer> keyedStream = map.keyBy(OrderData::getUserId);

        //1、翻滚计数窗口，每5个用户购买行为事件，统计购买总数，每当窗口填满5个元素，窗口触发计算
//        SingleOutputStreamOperator<OrderData> count = keyedStream.countWindow(5).sum("buyCnt");
//        count.print();
        //2、滑动计数窗口，最近5个用户购买行为事件，相同userId每出现3次进行统计
        SingleOutputStreamOperator<OrderData> count2 = keyedStream.countWindow(5, 3).sum("buyCnt");
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
