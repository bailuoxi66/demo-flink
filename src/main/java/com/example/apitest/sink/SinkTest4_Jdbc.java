package com.example.apitest.sink;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkTest4_Jdbc {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("C:\\Users\\laneliang\\IdeaProjects\\flinktutorial\\src\\main\\resources\\sourceFile.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.addSink(new MyJdbcSink());
        env.execute();
    }

    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {

        // 声明链接和预编译语句
        Connection connection = null;
        PreparedStatement insertStatement = null;
        PreparedStatement updateStatement = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
            insertStatement = connection.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)");
            updateStatement = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        // 每来一条数据，调用链接，执行sql
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            // 直接执行更新语句，如果没有更新成功就插入
            updateStatement.setDouble(1, value.getTemperature());
            updateStatement.setString(2, value.getId());
            updateStatement.execute();

            if (updateStatement.getUpdateCount() == 0) {
                insertStatement.setString(1, value.getId());
                insertStatement.setDouble(2, value.getTemperature());
                insertStatement.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStatement.close();
            updateStatement.close();
            connection.close();
        }
    }
}
