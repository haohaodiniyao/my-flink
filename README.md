flink版本：1.12.7
https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/connectors/jdbc.html
https://bahir.apache.org/docs/flink/1.0/flink-streaming-redis/

从socket获取数据
nc -lk 9999

watermark水印
http://wuchong.me/blog/2018/11/18/flink-tips-watermarks-in-apache-flink-made-easy/

Flink1.12版本后建议使用assignTimestampsAndWatermarks(WatermarkStrategy)的方式生成watermark，
之前使用的assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks)
以及assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)均不再被推荐使用
