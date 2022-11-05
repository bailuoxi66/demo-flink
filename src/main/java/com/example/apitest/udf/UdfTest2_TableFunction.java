package com.example.apitest.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.example.apitest.beans.SensorReading;

public class UdfTest2_TableFunction {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置为1
        env.setParallelism(1);

        // 创建Table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 读取数据
        DataStream<String> inputStream = env.readTextFile("E:\\Tech\\TTTTT\\demo-flink\\src\\main\\resources\\sensor.txt");

        // 2. 转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3. 将流转换为表
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature");

        // 4. 自定义表函数，实现将id拆分，并输出（word, length）
        Split split = new Split("_");
        // 需要在环境中注册UDF
        tableEnv.registerFunction("split", split);

        // 4.1 table API
        Table resultTable = sensorTable
            .joinLateral("split(id) as (word, length)")
            .select("id, ts, word, length");

        // 4.2 SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, ts, word, length " +
            " from sensor, lateral table(split(id)) as splitid(word, length)");

        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义 Table Function
    public static class Split extends TableFunction<Tuple2<String, Integer>> {

        // 定义属性，分隔符
        private String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }

        public void eval(String str) {
            for (String s : str.split(separator)) {
                collect(new Tuple2<>(s, s.length()));
            }
        }
    }
}
