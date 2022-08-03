package com.example.apitest.source;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author laneliang
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setParallelism(1);        会导致数据全有序

        // 从集合中读取数据
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8)
                , new SensorReading("sensor_6", 1547718201L, 15.4)
                , new SensorReading("sensor_7", 1547718202L, 6.7)
                , new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        DataStream<Integer> intStream = env.fromElements(1, 2, 4, 6, 9);

        // 打印输出, 传递的参数，表示当前输入的流的名称
        dataStream.print("data");
        intStream.print("int");

        // 执行
        env.execute();
    }
}
