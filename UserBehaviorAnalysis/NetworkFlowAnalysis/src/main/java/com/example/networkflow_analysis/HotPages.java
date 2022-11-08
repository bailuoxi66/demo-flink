package com.example.networkflow_analysis;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.text.SimpleDateFormat;

import com.example.networkflow_analysis.beans.ApacheLogEvent;
import com.example.networkflow_analysis.beans.PageViewCount;

public class HotPages {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取文件，转换成POJO
        URL resource = HotPages.class.getResource("/apache.log");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<ApacheLogEvent> dataStream = inputStream
            .map(line -> {
                String[] fields = line.split(" ");
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
                return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.minutes(1)) {
                @Override
                public long extractTimestamp(ApacheLogEvent element) {
                    return element.getTimestamp();
                }
            });

        // 分组开窗聚合
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
            // 过滤get请求
            .filter(data -> "GET".equals(data.getMethod()))
            // 按照url分组
            .keyBy(ApacheLogEvent::getUrl)
            .timeWindow(Time.minutes(10), Time.seconds(5))
            .aggregate(new PageCountAgg(), new PageCountResult());

        // 收集同一窗口count数据，排序输出
        DataStream<String> resultStream = windowAggStream.keyBy(PageViewCount::getWindowEnd)
            .process(new TopNHotPages(3));

        resultStream.print();
        env.execute("hot pages job");

    }
}
