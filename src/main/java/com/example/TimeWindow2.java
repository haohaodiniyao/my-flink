package com.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * 事件时间
 */
@Slf4j
public class TimeWindow2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<OrderData> map = textStream.map(new MapFunction<String, OrderData>() {
            @Override
            public OrderData map(String s) throws Exception {
                //1,1,1
                //1,1,1
                String[] arr = s.split(",");
                return new OrderData(Integer.valueOf(arr[0]), Integer.valueOf(arr[1]),Long.valueOf(arr[2]));
            }
        });
        OutputTag<OrderData> side = new OutputTag<OrderData>("side"){

        };
        SerializableTimestampAssigner<OrderData> serializableTimestampAssigner = (element, recordTimestamp) -> element.getTs();
        SingleOutputStreamOperator<OrderData> watermarks = map.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderData>forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(serializableTimestampAssigner));
        watermarks.print("数据加水印");
        //分组
        SingleOutputStreamOperator<OrderData> result = watermarks
                .keyBy(OrderData::getUserId)
                //1、滚动时间窗口，无重叠，窗口大小1分钟
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sideOutputLateData(side)
                .apply(new WindowFunction<OrderData, OrderData, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer integer, TimeWindow timeWindow, Iterable<OrderData> iterable, Collector<OrderData> collector) throws Exception {
                        System.out.println("窗口["+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timeWindow.getStart()))+","+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timeWindow.getEnd()))+"]当前时间="+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                        Integer userId = 0;
                        Integer buyCnt = 0;
                        for(OrderData orderData:iterable){
                            userId = orderData.getUserId();
                            buyCnt = buyCnt + orderData.getBuyCnt();
                            System.out.println("窗口数据 userId="+orderData.getUserId()+",buyCnt="+orderData.getBuyCnt()+",ts="+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(orderData.getTs())));
                        }
                        collector.collect(new OrderData(userId,buyCnt,System.currentTimeMillis()));
                    }
                });
        result.print("窗口聚合");
        result.getSideOutput(side).print("未进入窗口");
        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderData{
        private Integer userId;
        private Integer buyCnt;
        private Long ts;

        @Override
        public String toString() {
            return "OrderData{" +
                    "userId=" + userId +
                    ", buyCnt=" + buyCnt +
                    ", 事件时间=" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(ts)) +
                    ", 当前时间=" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) +
                    '}';
        }
    }
}
