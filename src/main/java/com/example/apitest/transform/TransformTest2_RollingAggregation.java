package com.example.apitest.transform;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author laneliang
 */
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("E:\\Tech\\demo-flink\\src\\main\\resources\\sensor.txt");
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        // KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);

        // 滚动聚合，取当前最大的温度值
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.maxBy("temperature");
        resultStream.print();
        env.execute();
    }
}
