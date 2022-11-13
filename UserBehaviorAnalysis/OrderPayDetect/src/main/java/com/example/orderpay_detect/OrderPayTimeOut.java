package com.example.orderpay_detect;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;

import com.example.orderpay_detect.beans.OrderEvent;
import com.example.orderpay_detect.beans.OrderResult;

public class OrderPayTimeOut {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取数据并转换成POJO类型
        URL resource = OrderPayTimeOut.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.readTextFile(resource.getPath())
            .map(line -> {
                String[] fields = line.split(",");
                return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
            }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                @Override
                public long extractAscendingTimestamp(OrderEvent element) {
                    return element.getTimestamp() * 1000L;
                }
            });

        // 定义一个带时间限制的模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        // 2. 侧输出流标签，用来表示超时事件
        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};

        // 3. 将pattern应用到输入数据流上，得到pattern stream
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(OrderEvent::getOrderId), orderPayPattern);

        // 4. 调用select方法，实现对匹配复杂事件和超时复杂事件的提取和处理
        SingleOutputStreamOperator<Object> resultStream = patternStream
            .select(orderTimeoutTag, new OrderTimeoutSelect(), new OrderPaySelect());

        resultStream.print();
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");
        env.execute("order timeout detect job");
    }
}
