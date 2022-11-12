package com.example.loginfail_detect;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

import com.example.loginfail_detect.beans.LoginEvent;

public class LoginFail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        URL resource = LoginFail.class.getResource("/LoginLog.csv");
        DataStream<LoginEvent> loginEventStream = env.readTextFile(resource.getPath())
            .map(line -> {
                String[] fields = line.split(",");
                return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
            }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                @Override
                public long extractTimestamp(LoginEvent element) {
                    return element.getTimestamp() * 1000L;
                }
            });

        // 自定义处理函数检测连续登录失败事件
        SingleOutputStreamOperator<Object> warningStream = loginEventStream.keyBy(LoginEvent::getUserId)
            .process(new LoginFailDetectWarning());

        warningStream.print();
        env.execute("login fail detect job");
    }
}
