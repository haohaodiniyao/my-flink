package com.example;

import com.google.inject.internal.cglib.core.$CollectionUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Watermaker
 * 水印机制
 * 水位线
 *
 * 就是一个时间戳
 * Watermaker计算 = 当前窗口的最大事件时间 - 延迟时间
 *
 * Watermaker作用
 * 之前窗口按照系统时间触发计算10：00：00 -  10：00：10
 * 10：00：10触发窗口计算，导致延迟到达数据丢失
 * 有了Watermaker可以按照Watermaker触发计算
 */
public class WatermakerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<MyData> source = env.addSource(new SourceFunction<MyData>() {
            boolean flag = true;

            @Override
            public void run(SourceContext<MyData> sourceContext) throws Exception {
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = 1;
                    int money = new Random().nextInt(101);
                    long eventTime = System.currentTimeMillis() - new Random().nextInt(5) * 1000;
                    System.out.println("发送数据："+new SimpleDateFormat("HH:mm:ss").format(eventTime));
                    sourceContext.collect(new MyData(orderId, "u_" + userId, eventTime, money));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置watermaker
        SingleOutputStreamOperator<MyData> watermarks = source.assignTimestampsAndWatermarks(WatermarkStrategy
                .<MyData>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((myData, timestamp) -> myData.getEventTime()));
        //分组
        KeyedStream<MyData, String> keyedStream = watermarks.keyBy(MyData::getUserId);

        //1、分组之后，每5秒统计一次，最近5秒(窗口大小)，用户订单总金额（滚动窗口）
        SingleOutputStreamOperator<String> result = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5))).apply(new WindowFunction<MyData, String, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow timeWindow, Iterable<MyData> iterable, Collector<String> collector) throws Exception {
                List<String> list = new ArrayList<>();
                for (MyData myData : iterable) {
                    list.add(new SimpleDateFormat("HH:mm:ss").format(myData.getEventTime()));
                }
                String start = new SimpleDateFormat("HH:mm:ss").format(timeWindow.getStart());
                String end = new SimpleDateFormat("HH:mm:ss").format(timeWindow.getEnd());
                collector.collect("[" + start + " - " + end + "]窗口事件：" + list.toString());
            }
        });
        result.print();
        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MyData{
        private String orderId;
        private String userId;
        private Long eventTime;
        private Integer money;
    }
}
