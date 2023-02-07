package com.example.apitest.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.example.apitest.beans.SensorAddrReading;

/**
 * @author laneliang
 */
public class WindowTest5_EventTimeWindow {
    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从socket读取数据
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7772);


        DataStream<SensorAddrReading> dataStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorAddrReading(fields[0], fields[1], new Long(fields[2]), new Double(fields[3]));
        })
                // 乱序数据设置时间戳和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorAddrReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorAddrReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                })
                ;

        SingleOutputStreamOperator<SensorAddrReading> minTemperature = dataStream.keyBy("id", "area")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .aggregate(new TempCountAgg(), new WindowTempCountResult());
        minTemperature.print();

        env.execute();
    }

    // 实现自定义的汇聚温度结果
    public static class TempCountAgg implements AggregateFunction<SensorAddrReading, Double, Double> {

        @Override
        public Double createAccumulator() {
            return 0.0;
        }

        @Override
        public Double add(SensorAddrReading value, Double accumulator) {
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
    public static class WindowTempCountResult implements WindowFunction<Double, SensorAddrReading, Tuple, TimeWindow>{

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Double> input, Collector<SensorAddrReading> out) throws Exception {
            Long windowEnd = window.getStart();
            Double count = input.iterator().next();
            out.collect(new SensorAddrReading(tuple.getField(0), tuple.getField(1), windowEnd, count));
        }
    }
}
