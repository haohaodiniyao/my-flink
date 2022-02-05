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

public class WindowDemo3 {
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

        //1、session超时时间10秒，10秒内没有数据到来，触发上个窗口计算
        SingleOutputStreamOperator<MyData> count = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).sum("count");
        count.print();

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
