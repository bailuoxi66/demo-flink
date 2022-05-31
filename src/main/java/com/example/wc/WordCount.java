package com.example.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author laneliang
 */
public class WordCount {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "E:\\Tech\\demo-flink\\src\\main\\resources\\hello.txt";

        // DataSource是一个数据源，其实是一个Operator算子，本质上是DataSet
        // 因为DataSet是父类，所以这里可以直接写DataSet
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        AggregateOperator<Tuple2<String, Integer>> result = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);

        result.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按照空格进行分词
            String[] words = value.split(" ");
            for (String word:words){
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
