package com.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSet = executionEnvironment.readTextFile("D:\\ushareit\\IdeaProjects\\push\\my-flink\\src\\main\\resources\\hello.txt");
        DataSet<Tuple2<String,Integer>> result = dataSet.flatMap(new MyFlatMap()).groupBy(0).sum(1);
        result.print();
    }
    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for(String word : words){
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
