package com.example.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author laneliang
 * 实时场景wordCount
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setParallelism(1);

        // 从socket 中读取数据
        // 使用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        // keyBy是根据当前key的hashcode对数据进行重分区的操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream =
                inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1).setParallelism(2);

        resultStream.print().setParallelism(1);

        // 请注意：当前是流数据场景，来一个处理一个，上面提前定义的是数据的操作处理流程，所以需要先将服务启动，然后等待数据，处理数据
        // 上述只是定义任务

        // 执行任务
        env.execute();
    }
}
