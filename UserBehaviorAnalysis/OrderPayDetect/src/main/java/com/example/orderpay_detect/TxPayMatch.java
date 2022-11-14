package com.example.orderpay_detect;

import org.apache.calcite.linq4j.Ord;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

import com.example.orderpay_detect.beans.OrderEvent;
import com.example.orderpay_detect.beans.ReceiptEvent;

public class TxPayMatch {

    // 定义侧输出流标签
    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatched-pays"){};
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatched-receipts"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取数据并转换成POJO类型

        // 读取订单支付事件
        URL orderResource = TxPayMatch.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.readTextFile(orderResource.getPath())
            .map(line -> {
                String[] fields = line.split(",");
                return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
            }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                @Override
                public long extractAscendingTimestamp(OrderEvent element) {
                    return element.getTimestamp() * 1000L;
                }
            }).filter(data -> !"".equals(data.getTxId()));  // 交易id不为空，必须是pay事件

        // 读取到账事件消息
        URL receiptResource = TxPayMatch.class.getResource("/ReceiptLog.csv");
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = env.readTextFile(receiptResource.getPath())
            .map(line -> {
                String[] fields = line.split(",");
                return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
            }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                @Override
                public long extractAscendingTimestamp(ReceiptEvent element) {
                    return element.getTimestamp() * 1000L;
                }
            });
        
        // 将两条流进行连接合并，进行匹配处理.不匹配的事件，输出到侧输出流标签
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream.keyBy(OrderEvent::getTxId)
            .connect(receiptEventStream.keyBy(ReceiptEvent::getTxId))
            .process(new TxPayMatchDetect());

        resultStream.print();
        resultStream.getSideOutput(unmatchedPays).print("unmatched-pays");
        resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts");

        env.execute("tx match detect job");
    }

    // 实现自定义CoProcessFunction
    public static class TxPayMatchDetect extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>{

        // 定义状态，保存当前已经到来的订单支付事件和到账时间
        ValueState<OrderEvent> payState;
        ValueState<ReceiptEvent> receiptState;

        @Override
        public void open(Configuration parameters) throws Exception {
            payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("pay", OrderEvent.class));
            receiptState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt", ReceiptEvent.class));
        }

        @Override
        public void processElement1(OrderEvent pay, Context ctx,
                                    Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 订单支付事件来了，判断是否已经有对应的到账事件
            ReceiptEvent receipt = receiptState.value();
            if (receipt != null){
                // 如果receipt不为空，说明到账事件已经来过，输出匹配事件，清空状态
                out.collect(new Tuple2<>(pay, receipt));
                payState.clear();
                receiptState.clear();
            } else {
                // 如果receipt没来， 注册一个定时器，开始等待
                ctx.timerService().registerEventTimeTimer((pay.getTimestamp() + 5) * 1000L );
                payState.update(pay  );
            }
        }

        @Override
        public void processElement2(ReceiptEvent receipt, CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.Context ctx,
                                    Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 订单支付事件来了，判断是否已经有对应的到账事件
            OrderEvent pay = payState.value();
            if (pay != null){
                // 如果pay不为空，说明支付事件已经来过，输出匹配事件，清空状态
                out.collect(new Tuple2<>(pay, receipt));
                payState.clear();
                receiptState.clear();
            } else {
                // 如果receipt没来， 注册一个定时器，开始等待
                ctx.timerService().registerEventTimeTimer((receipt.getTimestamp() + 5) * 1000L );
                receiptState.update(receipt  );
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                            Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 定时器触发，可能是有一个事件每来，不匹配。也可能是都来过了，已经输出并清空状态
            if (payState.value() != null){
                ctx.output(unmatchedPays, payState.value());
            }
            if (receiptState.value() != null){
                ctx.output(unmatchedReceipts, receiptState.value());
            }

            // 清空状态
            payState.clear();
            receiptState.clear();
        }
    }
}
