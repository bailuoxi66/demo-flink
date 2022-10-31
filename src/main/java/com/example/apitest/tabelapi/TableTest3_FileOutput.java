package com.example.apitest.tabelapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TableTest3_FileOutput {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        String filePath = "E:\\Tech\\TTTTT\\demo-flink\\src\\main\\resources\\sensor.txt";
        tableEnv.connect( new FileSystem().path(filePath))
            .withFormat( new Csv())
            .withSchema( new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp", DataTypes.BIGINT())
                .field("temp", DataTypes.DOUBLE()))
            .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");

//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();

        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select("id, temp")
            .filter("id == 'sensor_6'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
            .select("id, id.count as count, temp.avg as avgTemp");

        // 4. 输出到文件
        // 连接外部文件注册输出表
        String outputPath = "E:\\Tech\\TTTTT\\demo-flink\\src\\main\\resources\\out.txt";
        tableEnv.connect( new FileSystem().path(outputPath))
            .withFormat( new Csv())
            .withSchema( new Schema()
                .field("id", DataTypes.STRING())
                .field("temperature", DataTypes.DOUBLE()))
            .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        env.execute();

    }
}
