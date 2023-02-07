package com.example.apitest.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.example.apitest.beans.SensorReading;

/**
 * @author laneliang
 */
public class WindowTest4_EventTimeWindow {
    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从socket读取数据
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7772);


        DataStream<SensorReading> dataStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
                // 乱序数据设置时间戳和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                })
                ;

        SingleOutputStreamOperator<SensorReading> minTemperature = dataStream.keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .aggregate(new TempCountAgg(), new WindowTempCountResult());
        minTemperature.print();

        env.execute();
    }

    // 实现自定义的汇聚温度结果
    public static class TempCountAgg implements AggregateFunction<SensorReading, Double, Double> {

        @Override
        public Double createAccumulator() {
            return 0.0;
        }

        @Override
        public Double add(SensorReading value, Double accumulator) {
            return value.getTemperature() + accumulator;
        }

        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }

    // 自定义全窗口函数
    public static class WindowTempCountResult implements WindowFunction<Double, SensorReading, String, TimeWindow>{

        @Override
        public void apply(String key, TimeWindow window, Iterable<Double> input, Collector<SensorReading> out)
            throws Exception {
            Long windowEnd = window.getStart();
            Double count = input.iterator().next();
            out.collect(new SensorReading(key, windowEnd, count));
        }
    }
}
