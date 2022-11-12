package com.example.market_analysis;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;

import com.example.market_analysis.beans.AdClickEvent;
import com.example.market_analysis.beans.AdCountViewByProvince;
import com.example.market_analysis.beans.BlackListUserWarning;

public class AdStatisticsByProvince {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 从文件中读取数据
        URL resource = AdStatisticsByProvince.class.getResource("/AdClickLog.csv");
        DataStream<AdClickEvent> adClickEventDataStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                @Override
                public long extractAscendingTimestamp(AdClickEvent element) {
                    return element.getTimestamp() * 1000L;
                }
            });

        // 2. 基于省份分组、开窗聚合
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = adClickEventDataStream
            .keyBy("userId", "adId")
            .process(new FilterBlackListUser(100));

        // 3. 基于省份分组，开窗聚合
        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultStream = filterAdClickStream.keyBy(AdClickEvent::getProvince)
            .timeWindow(Time.hours(1), Time.seconds(5))
            .aggregate(new AdCountAgg(), new AdCountResult());

        //adCountResultStream.print();

        filterAdClickStream.getSideOutput(new OutputTag<BlackListUserWarning>("blackList"){}).print("blacklist-user");
        env.execute("ad count by province job");
    }

    public static class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince, String, TimeWindow>{

        @Override
        public void apply(String province, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new AdCountViewByProvince(province, windowEnd, count));
        }
    }

    public static class FilterBlackListUser extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent>{
        private Integer countUpperBound;

        public FilterBlackListUser(Integer countUpperBound){
            this.countUpperBound = countUpperBound;
        }

        // 定义状态，保存当前用户对某一广告的点击次数
        ValueState<Long> countState;
        // 定义一个状态标志，保存当前用户是否已经被发送到了黑名单里了
        ValueState<Boolean> isSentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-count", Long.class, 0L));
            isSentState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent", Boolean.class, false));
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void processElement(AdClickEvent value, KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent>.Context ctx,
                                   Collector<AdClickEvent> out) throws Exception {
            // 判断当前用户对同一广告的点击次数，如果不够上限，就count+1正常输出；如果达到上限，直接过滤掉，并侧输出流输出黑名单
            // 首先获取当前count值
            Long curCount = countState.value();

            // 1. 判断是否是第一个数据，如果是的话，注册一个第二天0点的定时器
            if (curCount == 0){
                Long ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000) - (8 * 60 * 60 * 1000);
                ctx.timerService().registerEventTimeTimer(ts);
            }

            // 2. 判断是否报警
            if (curCount >= countUpperBound){
                // 判断是否输出到黑名单过，如果没有的话，就输出到侧输出流
                if (!isSentState.value()){
                    isSentState.update(true);
                    ctx.output(new OutputTag<BlackListUserWarning>("blackList"){},
                        new BlackListUserWarning(value.getUserId(), value.getAdId(), "click over " + countUpperBound + " times"));
                }

                return;
            }

            // 如果没有返回，点击次数加1，更新状态，正常输出当前数据到主流
            countState.update(curCount + 1);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent>.OnTimerContext ctx, Collector<AdClickEvent> out)
            throws Exception {
            countState.clear();
            isSentState.clear();
        }
    }
}
