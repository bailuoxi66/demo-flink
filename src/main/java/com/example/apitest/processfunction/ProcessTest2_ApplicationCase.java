package com.example.apitest.processfunction;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessTest2_ApplicationCase {

    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从socket读取数据
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7772);


        DataStream<SensorReading> dataStream = inputDataStream.map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
                });

        // 测试keyedProcessFunction，先分组然后自定义处理
        dataStream.keyBy("id")
                .process(new TempConsIncreWarning(10))
                .print();

        env.execute();
    }

    // 实现自定义处理函数，检测一段时间的温度连续上升，实现报警
    public static class TempConsIncreWarning extends KeyedProcessFunction<Tuple, SensorReading, String> {

        // 实现自定义私有属性，当前统计的时间间隔
        private Integer internal;

        public TempConsIncreWarning(Integer internal) {
            this.internal = internal;
        }

        // 定义状态，保存上一次温度值，定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-temp", Double.class, Double.MIN_VALUE));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));
        }

        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<Tuple, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {

            // 取出状态
            Double lastTemp = lastTempState.value();
            Long ts = timerState.value();

            // 如果温度上升且没有定时器，注册10s后的定时器，开始等待
            if (value.getTemperature() > lastTemp && ts == null){
                // 计算出定时时间器
                long res = ctx.timerService().currentProcessingTime() + internal * 1000;
                ctx.timerService().registerProcessingTimeTimer(res);

                timerState.update(res);
            }

            // 如果温度下降，那么删除定时器
            if (value.getTemperature() < lastTemp && ts != null){
                ctx.timerService().deleteProcessingTimeTimer(ts);

                timerState.clear();
            }

            // 更新温度状态
            lastTempState.update(value.getTemperature());

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出报警信息
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续" + internal + "s上升");
            timerState.clear();
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
