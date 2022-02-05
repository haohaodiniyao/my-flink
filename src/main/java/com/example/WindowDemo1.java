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

public class WindowDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<MyData> map = textStream.map(new MapFunction<String, MyData>() {
            @Override
            public MyData map(String s) throws Exception {
                String[] arr = s.split(",");
                return new MyData(Integer.valueOf(arr[0]), Integer.valueOf(arr[1]));
            }
        });
        //分组
        KeyedStream<MyData, Integer> keyedStream = map.keyBy(MyData::getId);

        //1、分组之后，每5秒统计一次，最近5秒(窗口大小)，各个红绿灯通过汽车数量（滚动窗口）
        SingleOutputStreamOperator<MyData> count1 = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum("count");
        count1.print();
        //2、分组之后，每5秒统计一次，最近10秒（窗口大小），各个红绿灯通过汽车数量（滑动窗口）
//        SingleOutputStreamOperator<MyData> count2 = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).sum("count");
//        count2.print();

        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MyData{
        private Integer id;//红绿灯编号
        private Integer count;//通过车辆数
    }
}
