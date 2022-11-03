package com.example.apitest.tabelapi;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.example.apitest.beans.SensorReading;

public class TableTest5_TimeAndWindow {

    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读取数据
        DataStreamSource<String> inputStream = env.readTextFile("E:\\Tech\\TTTTT\\demo-flink\\src\\main\\resources\\sensor.txt");

        // 3. 转成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] str = line.split(",");
            return new SensorReading(str[0], new Long(str[1]), new Double(str[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 4. 将流转换成表，定义时间特性
        // Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, pt.proctime");
        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");
        dataTable.printSchema();
        tableEnv.toAppendStream(dataTable, Row.class).print();

        env.execute();
    }
}
