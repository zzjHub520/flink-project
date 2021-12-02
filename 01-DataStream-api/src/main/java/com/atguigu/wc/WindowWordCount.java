package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class WindowWordCount {

//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<Tuple2<String, Integer>> dataStream = env.socketTextStream("localhost", 9999)
//                .flatMap(new Splitter())
//                .keyBy(0)
//                .timeWindow(Time.seconds(10))
//                .sum(1);
//
//        dataStream.print();
//
//        env.execute("Window WordCount");
//    }
//
//    static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
//        @Override
//        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//            String[] words = value.split("\\W+");
//            for (String word : words) {
//                out.collect(new Tuple2<String, Integer>(word, 1));
//            }
//        }
//    }



    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.of(5, TimeUnit.SECONDS))
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}