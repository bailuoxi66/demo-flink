package com.example.apitest.state;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class StateTest4_FaultTolerance {

    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 状态后端配置
        env.setStateBackend( new MemoryStateBackend());
        env.setStateBackend( new FsStateBackend(""));
        env.setStateBackend( new RocksDBStateBackend(""));

        // 从socket读取数据
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7772);


        DataStream<SensorReading> dataStream = inputDataStream.map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
                });

        dataStream.print();
        env.execute();
    }
}
