package com.example.apitest.sink;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class SinkTest3_Es {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("C:\\Users\\laneliang\\IdeaProjects\\flinktutorial\\src\\main\\resources\\sourceFile.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义es的链接配置
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));

        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunction()).build());
        env.execute();
    }

    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading>{

        @Override
        public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            // 定义写入的数据
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id", sensorReading.getId());
            dataSource.put("temp", sensorReading.getTemperature().toString());
            dataSource.put("ts", sensorReading.getTimestamp().toString());

            // 创建请求，作为向es发起的写入命令
            IndexRequest indexRequest = Requests.indexRequest().index("sensor").type("readingdata").source(dataSource);

            // 用index发送请求
            requestIndexer.add(indexRequest);
        }
    }
}
