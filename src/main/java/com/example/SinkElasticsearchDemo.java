package com.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

/**
 * sink数据到MySQL
 */
public class SinkElasticsearchDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<User> input = env.fromElements(new User(102L,"wangwu",new Date()));

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 55001, "http"));

// use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<User> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<User>() {
                    public IndexRequest createIndexRequest(User element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("userName", element.getUserName());
                        json.put("id", element.getId());
                        json.put("createTime", element.getCreateTime());

                        return Requests.indexRequest()
                                .index("my-index")
                                .type("my-type")
                                .source(json);
                    }

                    @Override
                    public void process(User element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

// configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

// provide a RestClientFactory for custom configuration on the internally created REST client


//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders(...)
//                    restClientBuilder.setMaxRetryTimeoutMillis(...)
//                    restClientBuilder.setPathPrefix(...)
//                    restClientBuilder.setHttpClientConfigCallback(...)
//                }
//        );

// finally, build and add the sink to the job's pipeline
        input.addSink(esSinkBuilder.build());
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class User{
        private Long id;
        private String userName;
        private Date createTime;
    }
}
