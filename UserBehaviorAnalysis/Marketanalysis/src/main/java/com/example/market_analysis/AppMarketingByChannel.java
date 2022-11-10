package com.example.market_analysis;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.example.market_analysis.beans.MarketUserBehavior;

public class AppMarketingByChannel {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 从自定义数据源中读取数据
        DataStream<MarketUserBehavior> dataStream = env.addSource(new SimulatedMarketingUserBehaviorSource());
    }

    // 实现自定义的模拟市场用户行为数据
    public static class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketUserBehavior>{

        // 控制是否正常运行的标识符
        Boolean running = true;

        // 定义用户行为和渠道的范围
        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        List<String> channelList = Arrays.asList("app store", "wechat", "weibo");

        Random  random = new Random();

        @Override
        public void run(SourceContext<MarketUserBehavior> ctx) throws Exception {
            while (running){
                // 随机生成所有的字段
                Long id = random.nextLong();
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = channelList.get(random.nextInt(channelList.size()));
                Long timestamp = System.currentTimeMillis();

                // 发出数据
                ctx.collect(new MarketUserBehavior(id, behavior, channel, timestamp));

                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
