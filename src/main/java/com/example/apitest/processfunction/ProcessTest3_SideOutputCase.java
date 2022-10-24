package com.example.apitest.processfunction;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessTest3_SideOutputCase {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7772);

        // 转换成SensorReading类型，分配时间戳和watermark
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个OutputTag，用来表示低温侧输出流
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("lowTemp"){};

        // 测试ProcessFunction，自定义侧输出流，实现分流操作
        SingleOutputStreamOperator<SensorReading> highTemp = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
                // 判断温度，大于30度，高温输出到主流；低温输出到侧流
                if (value.getTemperature() > 30){
                    out.collect(value);
                } else {
                    ctx.output(outputTag, value);
                }
            }
        });

        highTemp.print("high-temp");
        highTemp.getSideOutput(outputTag).print("low-temp");

        env.execute();
    }
}
