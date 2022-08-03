package com.example.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author laneliang
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> dataStream = env.readTextFile("E:\\Tech\\demo-flink\\src\\main\\resources\\sensor.txt");

        dataStream.print();

        env.execute();
    }
}