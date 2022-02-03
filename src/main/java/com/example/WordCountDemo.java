package com.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * checkpoint
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/fault-tolerance/checkpointing/
 */
@Slf4j
public class WordCountDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        //checkpoint
        env.enableCheckpointing(1000);//每隔1秒执行1次checkpoint
        if(SystemUtils.IS_OS_MAC){
            env.setStateBackend(new FsStateBackend("file:///Users/yaokai/Downloads/tmp/checkpoint"));
        }
        //checkpoint最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //checkpoint过程出现错误，是否让整体任务失败默认rue
//        env.getCheckpointConfig().setFailOnCheckpointingErrors();
        //默认0，表示不容忍任何检查点失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        //是否清理检查点，表示cancel时是否保留当前的checkpoint，默认，checkpoint会在作业被cancel时删除
        //RETAIN_ON_CANCELLATION 作业取消，保留checkpoint
        //DELETE_ON_CANCELLATION 作业取消，删除checkpoint
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //直接使用默认值
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //checkpoint超时时间，如果checkpoint在60秒未完成说明checkpoint失败，则丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60*1000);//默认10分钟
        //同一时间多少checkpoint同时执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);//默认1
        //重启策略
        //1、配置了checkpoint，不配置，默认无限重启并自动恢复，可以解决小问题，但是可能隐藏真正bug
        //2、无重启策略
//        env.setRestartStrategy(RestartStrategies.noRestart());
        //3、固定延迟重启
        //最多重启3次，间隔10秒(如果job失败)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        //4、失败率重启
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3,//5分钟内3次失败，重启间隔3秒
//                Time.of(5,TimeUnit.MINUTES),
//                Time.of(3,TimeUnit.SECONDS)));

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String output = parameterTool.get("output","/Users/yaokai/Downloads/tmp/output");
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stringDataStreamSource.flatMap(new LineSplitter()).keyBy(t -> t.f0).sum(1);
//        sum.writeAsText(output, FileSystem.WriteMode.OVERWRITE);
        sum.print();
        env.execute();
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : line.split(" ")) {
                if(word.equals("bug")){
                    throw new Exception("bug...");
                }
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}