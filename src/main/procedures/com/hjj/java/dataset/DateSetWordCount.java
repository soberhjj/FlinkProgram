package com.hjj.java.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author huangJunJie 2021-10-19-11:43
 */
public class DateSetWordCount {
    public static void main(String[] args) throws Exception {
        //创建Flink运行的上下文环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //创建DataSet
        DataSet<String> input = env.fromElements("Hello Flink", "Hello Spark", "Hello Hadoop", "Flink", "Spark");

        DataSet<Tuple2<String, Integer>> res = input.flatMap(new LineSplitter()).groupBy(0).sum(1);

        res.print();

    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.toLowerCase().split(" ");
            for (String word : words) {
                out.collect(new Tuple2<String,Integer>(word,1));
            }
        }
    }
}




